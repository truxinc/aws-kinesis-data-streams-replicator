package consumer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;

public class Handler implements RequestHandler<KinesisEvent, StreamsEventResponse> {

    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    private static final CharsetDecoder utf8Decoder = StandardCharsets.UTF_8.newDecoder();

    private KinesisClient kinesisForwarder = null;
    private CloudWatchClient cw = null;
    private SnsClient sns = null;
    String currentRegion = System.getenv("AWS_REGION");

    public Handler() {
        kinesisForwarder = KinesisClient.builder()
                .region(Region.of(System.getenv("TARGET_STREAM_REPLICATION_REGION")))
                .httpClient(ApacheHttpClient.create())
                .build();
        cw = CloudWatchClient.builder()
                .region(Region.of(System.getenv("TARGET_STREAM_REPLICATION_REGION")))
                .httpClient(ApacheHttpClient.create())
                .build();
        sns = SnsClient.builder()
                .region(Region.of(currentRegion))
                .build();
    }

    @Override
    public StreamsEventResponse handleRequest(KinesisEvent kinesisEvent, Context context) {
        Instant start = Instant.now();
        List<StreamsEventResponse.BatchItemFailure> failures = new ArrayList<>();
        String streamName = getStreamName(kinesisEvent.getRecords().get(0));
            int batchSize = kinesisEvent.getRecords().size();
            AtomicInteger successful = new AtomicInteger();
            AtomicReference<String> currentSequenceNumber = new AtomicReference<>("");
            Instant lastApproximateArrivalInstant = null;
            try {
                for (KinesisEvent.KinesisEventRecord record : kinesisEvent.getRecords()) {
                    currentSequenceNumber.getAndSet(record.getKinesis().getSequenceNumber());
                    String actualDataString = utf8Decoder.decode(record.getKinesis().getData()).toString();
                    logger.info("Actual Record SequenceNumber: {}, Data: {}", currentSequenceNumber, actualDataString);

                    // Forward to other region
                    PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                            .streamName(streamName)
                            .partitionKey(record.getKinesis().getPartitionKey())
                            .data(SdkBytes.fromString(actualDataString, StandardCharsets.UTF_8))
                            .build();
                    kinesisForwarder.putRecord(putRecordRequest);
                    lastApproximateArrivalInstant = record.getKinesis().getApproximateArrivalTimestamp().toInstant();
                    successful.getAndIncrement();
                }
            } catch (Exception e) {
                failures.add(StreamsEventResponse.BatchItemFailure.builder().withItemIdentifier(currentSequenceNumber.get()).build());
                logger.error("Error while processing batch", e);
            }
            Instant end = Instant.now();
            int successfullyProcessed = successful.get();
            long timeTakenSeconds = Duration.between(start, end).toSeconds();
            long replicationLag = Duration.between(lastApproximateArrivalInstant, end).toSeconds();
            logger.info("TotalBatchSize {} Successful {} TimeTakenSeconds {} ReplicationLag {}", batchSize, successfullyProcessed, timeTakenSeconds, replicationLag);
            try {
                MetricDatum throughputMetricData = MetricDatum.builder()
                        .metricName("ThroughPut")
                        .dimensions(Dimension.builder()
                                .name("StreamName")
                                .value(streamName)
                                .build())
                        .value((double) successfullyProcessed)
                        .build();
                MetricDatum replicationLatencyMetricData = MetricDatum.builder()
                        .metricName("ReplicationLagSeconds")
                        .dimensions(Dimension.builder()
                                .name("StreamName")
                                .value(streamName)
                                .build())
                        .value((double) replicationLag)
                        .build();
                cw.putMetricData(PutMetricDataRequest.builder()
                        .namespace("KinesisCrossRegionReplication")
                        .metricData(asList(throughputMetricData, replicationLatencyMetricData))
                        .build());
            } catch (Throwable e) {
                logger.error("Error while publishing metric to cloudwatch");
            }
            if (!failures.isEmpty()) {
                // Construct the message to send to SNS
                StringBuilder message = new StringBuilder("Batch processing failures:\n");
                for (StreamsEventResponse.BatchItemFailure failure : failures) {
                    message.append("Item: ")
                        .append(failure.getItemIdentifier())
                        .append("\n");
                }
                // Publish message to SNS topic
                String topicArn = System.getenv("SNS_Topic_ARN");
                PublishRequest publishRequest = PublishRequest.builder()
                        .topicArn(topicArn)
                        .message(message.toString())
                        .subject("Kinesis Replication Batch Processing Failures")
                        .build();
                sns.publish(publishRequest);
            }
        return StreamsEventResponse.builder().withBatchItemFailures(failures).build();
    }

    private String getStreamName(KinesisEvent.KinesisEventRecord record) {
        return record.getEventSourceARN().split(":")[5].split("/")[1];
    }
}
