package consumer;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.Record;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.junit.Test;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class HandlerTest {

    private static final String SOURCE_ARN =
            "arn:aws:kinesis:us-east-2:111111111111:stream/test_stream/consumer/test_stream_replicator:1";
    private static final String THROTTLED = "ProvisionedThroughputExceededException";

    @Test
    public void sendsRecordsInChunksOfAtMost500() {
        FakeKinesis kinesis = new FakeKinesis(Set.of(), Set.of(), THROTTLED);
        FakeSns sns = new FakeSns();

        StreamsEventResponse response = handler(kinesis, sns).handleRequest(event(1200), new TestContext());

        assertThat(kinesis.requests).extracting(r -> r.records().size()).containsExactly(500, 500, 200);
        assertThat(kinesis.requests).allMatch(r -> r.streamName().equals("test_stream"));
        assertThat(response.getBatchItemFailures()).isEmpty();
        assertThat(sns.published).isEmpty();
    }

    @Test
    public void forwardsPayloadBytesUnchanged() {
        FakeKinesis kinesis = new FakeKinesis(Set.of(), Set.of(), THROTTLED);
        KinesisEvent event = event(1);
        byte[] notUtf8 = {(byte) 0xC3, (byte) 0x28, (byte) 0xFF};
        event.getRecords().get(0).getKinesis().setData(ByteBuffer.wrap(notUtf8));

        handler(kinesis, new FakeSns()).handleRequest(event, new TestContext());

        assertThat(kinesis.requests.get(0).records().get(0).data().asByteArray()).isEqualTo(notUtf8);
    }

    @Test
    public void retriesOnlyFailedEntries() {
        FakeKinesis kinesis = new FakeKinesis(Set.of(), Set.of("pk-1", "pk-3"), THROTTLED);

        StreamsEventResponse response = handler(kinesis, new FakeSns()).handleRequest(event(5), new TestContext());

        assertThat(kinesis.requests).extracting(r -> r.records().size()).containsExactly(5, 2);
        assertThat(kinesis.requests.get(1).records()).extracting(e -> e.partitionKey()).containsExactly("pk-1", "pk-3");
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    @Test
    public void reportsFirstPersistentlyFailedRecord() {
        FakeKinesis kinesis = new FakeKinesis(Set.of("pk-502"), Set.of("pk-504"), "InternalFailure");
        FakeSns sns = new FakeSns();

        StreamsEventResponse response = handler(kinesis, sns).handleRequest(event(505), new TestContext());

        assertThat(kinesis.requests).extracting(r -> r.records().size()).containsExactly(500, 5, 2, 1);
        assertThat(response.getBatchItemFailures())
                .extracting(StreamsEventResponse.BatchItemFailure::getItemIdentifier)
                .containsExactly("seq-502");
        assertThat(sns.published).hasSize(1);
    }

    @Test
    public void reportsThrottledRecordWithoutNotifying() {
        FakeKinesis kinesis = new FakeKinesis(Set.of("pk-2"), Set.of(), THROTTLED);
        FakeSns sns = new FakeSns();

        StreamsEventResponse response = handler(kinesis, sns).handleRequest(event(5), new TestContext());

        assertThat(kinesis.requests).extracting(r -> r.records().size()).containsExactly(5, 1, 1);
        assertThat(response.getBatchItemFailures())
                .extracting(StreamsEventResponse.BatchItemFailure::getItemIdentifier)
                .containsExactly("seq-2");
        assertThat(sns.published).isEmpty();
    }

    private static Handler handler(FakeKinesis kinesis, FakeSns sns) {
        return new Handler(kinesis, new CloudWatchClient() {
            @Override
            public PutMetricDataResponse putMetricData(PutMetricDataRequest request) {
                return PutMetricDataResponse.builder().build();
            }

            @Override
            public String serviceName() {
                return "cloudwatch";
            }

            @Override
            public void close() {
            }
        }, sns, "arn:aws:sns:us-east-2:111111111111:test-topic");
    }

    private static KinesisEvent event(int count) {
        List<KinesisEventRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Record kinesis = new Record();
            kinesis.setPartitionKey("pk-" + i);
            kinesis.setSequenceNumber("seq-" + i);
            kinesis.setData(ByteBuffer.wrap(("payload-" + i).getBytes()));
            kinesis.setApproximateArrivalTimestamp(new Date());
            KinesisEventRecord record = new KinesisEventRecord();
            record.setEventSourceARN(SOURCE_ARN);
            record.setKinesis(kinesis);
            records.add(record);
        }
        KinesisEvent event = new KinesisEvent();
        event.setRecords(records);
        return event;
    }

    private static class FakeKinesis implements KinesisClient {
        final List<PutRecordsRequest> requests = new ArrayList<>();
        private final Set<String> alwaysFail;
        private final Set<String> failOnce;
        private final String errorCode;
        private final Set<String> failedOnce = new HashSet<>();

        FakeKinesis(Set<String> alwaysFail, Set<String> failOnce, String errorCode) {
            this.alwaysFail = alwaysFail;
            this.failOnce = failOnce;
            this.errorCode = errorCode;
        }

        @Override
        public PutRecordsResponse putRecords(PutRecordsRequest request) {
            requests.add(request);
            List<PutRecordsResultEntry> results = new ArrayList<>();
            int failed = 0;
            for (PutRecordsRequestEntry entry : request.records()) {
                String key = entry.partitionKey();
                if (alwaysFail.contains(key) || (failOnce.contains(key) && failedOnce.add(key))) {
                    results.add(PutRecordsResultEntry.builder()
                            .errorCode(errorCode)
                            .errorMessage("Rejected by fake")
                            .build());
                    failed++;
                } else {
                    results.add(PutRecordsResultEntry.builder().sequenceNumber("out-" + key).shardId("shardId-0").build());
                }
            }
            return PutRecordsResponse.builder().records(results).failedRecordCount(failed).build();
        }

        @Override
        public String serviceName() {
            return "kinesis";
        }

        @Override
        public void close() {
        }
    }

    private static class FakeSns implements SnsClient {
        final List<PublishRequest> published = new ArrayList<>();

        @Override
        public PublishResponse publish(PublishRequest request) {
            published.add(request);
            return PublishResponse.builder().build();
        }

        @Override
        public String serviceName() {
            return "sns";
        }

        @Override
        public void close() {
        }
    }
}
