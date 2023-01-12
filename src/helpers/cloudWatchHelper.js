import logger from "loglevel";
import { CloudWatchLogsClient, CreateLogStreamCommand, PutLogEventsCommand, FilterLogEventsCommand  } from "@aws-sdk/client-cloudwatch-logs";
import { CloudWatchClient, GetMetricDataCommand } from "@aws-sdk/client-cloudwatch";
import config from "config";

const cloudwatch = new CloudWatchClient({ region: "us-east-1" });
const cloudwatchLogs = new CloudWatchLogsClient({ region: "us-east-1" });

export default class CloudWatchHelper {
  constructor(clouwatchLogGroupName) {
    this.logStreamName = null;
    this.nextSequenceToken = null;
    this.CLOUDWATCH_LOG_GROUP_NAME = clouwatchLogGroupName;
  }

  async initializeLogStream(logStreamName) {
    await cloudwatchLogs.send(new CreateLogStreamCommand({
      logGroupName: this.CLOUDWATCH_LOG_GROUP_NAME,
      logStreamName: logStreamName
    }));

    this.logStreamName = logStreamName;

    return true;
  }

  async logAndRegisterMessage(message) {
    if(this.logStreamName) {
      const { nextSequenceToken } = await cloudwatchLogs.send(new PutLogEventsCommand({
        logEvents: [{ message: message, timestamp: Date.now() }],
        logGroupName: this.CLOUDWATCH_LOG_GROUP_NAME,
        logStreamName: this.logStreamName,
        sequenceToken: this.nextSequenceToken
      }));

      this.nextSequenceToken = nextSequenceToken;
    } else {
      logger.warn('Log stream must be initialized to register logs');
    }

    return true;
  }

  static async getLastMetric({ metricDataQuery }) {
    const now = new Date();

    const {
      MetricDataResults: [{
        Values: [metricData],
      }],
    } = await cloudwatch.send(new GetMetricDataCommand({
      StartTime: new Date(now - 360000),
      EndTime: now,
      MetricDataQueries: [metricDataQuery],
    }));

    return metricData;
  }

  static async getLogMessages({ startTime, filterPattern } = {}) {
    const {
      events,
    } = await cloudwatchLogs.send(new FilterLogEventsCommand({
      logGroupName: this.CLOUDWATCH_LOG_GROUP_NAME ?? config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME,
      filterPattern,
      startTime,
    }));

    return events.map(({ message }) => JSON.parse(message));
  }

  static async getCredits({ instanceId }){
    const now = new Date();

    const {
      MetricDataResults: [{
        Values: [metricData],
      }],
    } = await cloudwatch.send(new GetMetricDataCommand({
      StartTime: new Date(now - 360000),
      EndTime: now,
      MetricDataQueries: [
        {
        Id: "cpuCreditBalance",
          MetricStat: {
            Metric: {
              Dimensions: [
                {
                  Name: "InstanceId",
                  Value: instanceId
                },
              ],
              MetricName: "CPUCreditBalance",
              Namespace: "AWS/EC2"
            },
            Period: 60,
            Stat: "Maximum",
          },
        }
      ],
    }));

    return metricData ?? 0;
  }
}