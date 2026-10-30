package consumer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Handler implements RequestHandler<KinesisEvent, StreamsEventResponse> {

    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    private static final int MAX_RECORDS_PER_PUT = 500;
    private static final int MAX_BYTES_PER_PUT = 5 * 1024 * 1024;
    private static final int MAX_PUT_ATTEMPTS = 3;
    private static final long RETRY_BACKOFF_MILLIS = 100;

    private final KinesisClient kinesisForwarder;
    private final CloudWatchClient cw;
    private final SnsClient sns;
    private final String snsTopicArn;

    public Handler() {
        this(KinesisClient.builder()
                        .region(Region.of(System.getenv("TARGET_STREAM_REPLICATION_REGION")))
                        .httpClient(Apache5HttpClient.create())
                        .build(),
                CloudWatchClient.builder()
                        .region(Region.of(System.getenv("TARGET_STREAM_REPLICATION_REGION")))
                        .httpClient(Apache5HttpClient.create())
                        .build(),
                SnsClient.builder()
                        .region(Region.of(System.getenv("AWS_REGION")))
                        .build(),
                System.getenv("SNS_Topic_ARN"));
    }

    Handler(KinesisClient kinesisForwarder, CloudWatchClient cw, SnsClient sns, String snsTopicArn) {
        this.kinesisForwarder = kinesisForwarder;
        this.cw = cw;
        this.sns = sns;
        this.snsTopicArn = snsTopicArn;
    }

    @Override
    public StreamsEventResponse handleRequest(KinesisEvent kinesisEvent, Context context) {
        Instant start = Instant.now();
        List<KinesisEvent.KinesisEventRecord> records = kinesisEvent.getRecords();
        String streamName = getStreamName(records.get(0));

        int successful = 0;
        String failedSequenceNumber = null;
        for (List<KinesisEvent.KinesisEventRecord> chunk : chunk(records)) {
            int sent = putChunkReturningSentCount(streamName, chunk);
            successful += sent;
            if (sent < chunk.size()) {
                failedSequenceNumber = chunk.get(sent).getKinesis().getSequenceNumber();
                break;
            }
        }

        Instant end = Instant.now();
        Instant lastArrival = records.get(Math.max(successful - 1, 0)).getKinesis().getApproximateArrivalTimestamp().toInstant();
        long replicationLag = Duration.between(lastArrival, end).toSeconds();
        logger.info("TotalBatchSize {} Successful {} TimeTakenSeconds {} ReplicationLag {}",
                records.size(), successful, Duration.between(start, end).toSeconds(), replicationLag);
        publishMetrics(streamName, successful, replicationLag);

        if (failedSequenceNumber == null) {
            return StreamsEventResponse.builder().withBatchItemFailures(new ArrayList<>()).build();
        }
        notifyFailure(streamName, failedSequenceNumber);
        List<StreamsEventResponse.BatchItemFailure> failures = new ArrayList<>();
        failures.add(StreamsEventResponse.BatchItemFailure.builder().withItemIdentifier(failedSequenceNumber).build());
        return StreamsEventResponse.builder().withBatchItemFailures(failures).build();
    }

    private static List<List<KinesisEvent.KinesisEventRecord>> chunk(List<KinesisEvent.KinesisEventRecord> records) {
        List<List<KinesisEvent.KinesisEventRecord>> chunks = new ArrayList<>();
        List<KinesisEvent.KinesisEventRecord> current = new ArrayList<>();
        long currentBytes = 0;
        for (KinesisEvent.KinesisEventRecord record : records) {
            long size = record.getKinesis().getData().remaining()
                    + record.getKinesis().getPartitionKey().getBytes(StandardCharsets.UTF_8).length;
            if (!current.isEmpty() && (current.size() == MAX_RECORDS_PER_PUT || currentBytes + size > MAX_BYTES_PER_PUT)) {
                chunks.add(current);
                current = new ArrayList<>();
                currentBytes = 0;
            }
            current.add(record);
            currentBytes += size;
        }
        chunks.add(current);
        return chunks;
    }

    private int putChunkReturningSentCount(String streamName, List<KinesisEvent.KinesisEventRecord> chunk) {
        List<PutRecordsRequestEntry> entries = new ArrayList<>(chunk.size());
        List<Integer> pending = new ArrayList<>(chunk.size());
        for (int i = 0; i < chunk.size(); i++) {
            entries.add(PutRecordsRequestEntry.builder()
                    .partitionKey(chunk.get(i).getKinesis().getPartitionKey())
                    .data(SdkBytes.fromByteBuffer(chunk.get(i).getKinesis().getData()))
                    .build());
            pending.add(i);
        }

        for (int attempt = 1; ; attempt++) {
            List<PutRecordsRequestEntry> attemptEntries = new ArrayList<>(pending.size());
            for (int i : pending) {
                attemptEntries.add(entries.get(i));
            }
            List<PutRecordsResultEntry> results;
            try {
                results = kinesisForwarder.putRecords(PutRecordsRequest.builder()
                        .streamName(streamName)
                        .records(attemptEntries)
                        .build()).records();
            } catch (SdkException e) {
                logger.error("PutRecords to {} failed", streamName, e);
                return pending.get(0);
            }

            List<Integer> stillFailed = new ArrayList<>();
            PutRecordsResultEntry firstError = null;
            for (int j = 0; j < results.size(); j++) {
                if (results.get(j).errorCode() != null) {
                    stillFailed.add(pending.get(j));
                    if (firstError == null) {
                        firstError = results.get(j);
                    }
                }
            }
            if (stillFailed.isEmpty()) {
                return chunk.size();
            }
            if (attempt == MAX_PUT_ATTEMPTS) {
                logger.error("{} of {} records to {} still failing after {} attempts: {} {}", stillFailed.size(),
                        chunk.size(), streamName, attempt, firstError.errorCode(), firstError.errorMessage());
                return stillFailed.get(0);
            }
            pending = stillFailed;
            try {
                Thread.sleep(RETRY_BACKOFF_MILLIS * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return pending.get(0);
            }
        }
    }

    private void publishMetrics(String streamName, int successful, long replicationLag) {
        Dimension stream = Dimension.builder().name("StreamName").value(streamName).build();
        try {
            cw.putMetricData(PutMetricDataRequest.builder()
                    .namespace("KinesisCrossRegionReplication")
                    .metricData(
                            MetricDatum.builder().metricName("ThroughPut").dimensions(stream).value((double) successful).build(),
                            MetricDatum.builder().metricName("ReplicationLagSeconds").dimensions(stream).value((double) replicationLag).build())
                    .build());
        } catch (Throwable e) {
            logger.error("Error while publishing metric to cloudwatch", e);
        }
    }

    private void notifyFailure(String streamName, String failedSequenceNumber) {
        try {
            sns.publish(PublishRequest.builder()
                    .topicArn(snsTopicArn)
                    .subject("Kinesis Replication Batch Processing Failures")
                    .message("Batch processing failures on " + streamName + ":\nItem: " + failedSequenceNumber + "\n")
                    .build());
        } catch (SdkException e) {
            logger.error("Error while publishing failure notification for {}", streamName, e);
        }
    }

    private String getStreamName(KinesisEvent.KinesisEventRecord record) {
        return record.getEventSourceARN().split(":")[5].split("/")[1];
    }
}
