# Kinesis Data Streams cross-region replicator

Copies every record from a Kinesis data stream in its home region to a stream with the same name in a
DR region (us-west-2 for all Trux environments). There is one SAM stack per source stream.

Originally forked from the AWS sample `aws-samples/aws-kinesis-data-streams-replicator`; the
DynamoDB checkpoint and active-region tables from that sample are not used.

## How it works

Each stack (`template.yaml`) creates:

- an Enhanced Fan-Out consumer `<stream>_replicator` on the source stream, so replication gets its own
  2 MB/s per shard and does not compete with application consumers;
- the Lambda `<stream>-replication-lambda-function` (Java 25), attached to that consumer through an event
  source mapping (`StartingPosition: LATEST`, `ReportBatchItemFailures`, retries until the record expires);
- an IAM role allowing `kinesis:PutRecords` on the same-named stream in any region, CloudWatch metrics,
  and SNS publish;
- a CloudWatch alarm `<stream>-replication-lag` on the function's `IteratorAge` (over 1 hour for 15
  minutes), notifying `SNSTopic`.

For each invocation the handler (`KinesisReplicatorFunction/src/main/java/consumer/Handler.java`):

1. splits the batch, in order, into `PutRecords` calls of at most 500 records / 5 MB;
2. writes each call to the target region, passing the payload bytes and partition key through unchanged;
3. retries only the entries a call rejected (usually `ProvisionedThroughputExceededException`), up to three
   attempts with a short backoff;
4. if entries still fail, returns the first failed record's sequence number as the batch item failure, so
   Lambda resumes from that record. It publishes a notification to `SNSTopic` only when a failure is not
   throttling: throttled writes are retried by Lambda on the next invocation, and throttling that keeps
   replication behind is reported by the `<stream>-replication-lag` alarm instead;
5. publishes `ThroughPut` and `ReplicationLagSeconds` to the `KinesisCrossRegionReplication` namespace in
   the target region, and logs one summary line per batch.

Delivery is at least once. A retried batch can re-send records that were already written, and
records that a producer aggregated with the KPL share one sequence number, so they are retried together.
Writes are not strictly ordered: a retried entry lands after entries that succeeded on the first attempt.

The target stream must exist before the stack is deployed. Prod's Oregon streams are created by the
`prodOregon` branch of `truxinc/cdk` (`prodOregonKinesisStackCDK`).

## Configuration

`samconfig.toml` has one config env per source stream. Each sets the stack name, the source region, and
these parameters:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `InputStreamName` | — | Source stream; the target stream has the same name |
| `TargetStreamReplicationRegion` | — | DR region (`us-west-2`) |
| `SNSTopic` | — | Topic name in the source region for failure and lag notifications |
| `FunctionMemorySize` | 512 | Lambda memory in MB |
| `EventSourceBatchSize` | 8000 | Maximum records per invocation |

## Build and deploy

Needs JDK 25, Maven, and the SAM CLI. Deploying needs a profile that can create IAM roles and
CloudFormation stacks in the target account.

```shell script
sam build
sam deploy --config-env <stream_name>
```

`confirm_changeset = true` is set for every env, so review the change set before confirming it. Redeploying
an existing stack updates the function in place and keeps the event source mapping, so its checkpoint is kept.

Run the unit tests with `mvn test` in `KinesisReplicatorFunction/`.

## Checking that replication is complete

Compare the daily `AWS/Kinesis` `IncomingBytes` of the source stream with the same-named stream in the
target region; they should match within a percent or two. `IncomingRecords` does not match even when
replication is complete: producers that aggregate with the KPL send one Kinesis record for many user
records, and the Lambda receives and writes those user records individually.

`IteratorAge` on the function is the replication lag. If it reaches the source stream's retention period,
records expire before they are replicated.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
