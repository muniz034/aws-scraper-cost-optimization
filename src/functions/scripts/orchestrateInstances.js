import logger from "loglevel";
import { SQSClient, GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import minimist from "minimist";
import { CronJob } from "cron";
import config from "config";
import fs from "fs";

import sleep from "../../utils/sleep.js";
import ec2Pricing from "../../constants/ec2Pricing.js";
import InstancesHelper from "../../helpers/instancesHelper.js";
import CloudWatchHelper from "../../helpers/cloudWatchHelper.js";
import getLocalTime from "../../utils/getLocalTime.js";

async function getClusterMetrics({ startTime }) {
  const messages = await CloudWatchHelper.getLogMessages({
    filterPattern: '{ ($.instanceId != "local") && ($.averageMessageServiceTime = *) && ($.averageMessageProcessingTime = *)}',
    startTime,
  });

  const [
    averageClusterServiceTimeAccumulator,
    averageClusterProcessingTimeAccumulator,
  ] = messages.reduce(
    function (
      [averageMessageServiceTimeAccumulator, averageMessageProcessingTimeAccumulator],
      { averageMessageServiceTime, averageMessageProcessingTime }
    ) {
      return [
        averageMessageServiceTimeAccumulator + averageMessageServiceTime,
        averageMessageProcessingTimeAccumulator + averageMessageProcessingTime,
      ];
    },
    [0, 0],
  );

  const averageClusterServiceTime = averageClusterServiceTimeAccumulator / messages.length;
  const averageClusterProcessingTime = averageClusterProcessingTimeAccumulator / messages.length;

  return {
    averageClusterServiceTime,
    averageClusterProcessingTime,
  };
}

async function getApproximateNumberOfMessages() {
  const {
    Attributes: {
      ApproximateNumberOfMessages: approximateNumberOfMessages
    },
  } = await sqs.send(new GetQueueAttributesCommand({
    QueueUrl: config.get("AWS").SQS_QUEUE_URL,
    AttributeNames: ["ApproximateNumberOfMessages"]
  }));

  return parseInt(approximateNumberOfMessages);
}

const sqs = new SQSClient({ region: "us-east-1" });

logger.setLevel("info");

if(!config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME) throw new Error("CLOUDWATCH_LOG_GROUP_NAME enviroment variable expected");
if(!config.get("AWS").SQS_QUEUE_URL) throw new Error("SQS_QUEUE_URL enviroment variable expected");
if(!config.get("AWS").S3_RESULT_BUCKET_NAME) throw new Error("S3_RESULT_BUCKET_NAME enviroment variable expected");

let {
  _,
  sla,
  instanceType,
  parallelProcessingCapacity,
  privateKey,
  resultsPath,
  maximumClusterSize,
} = minimist(process.argv.slice(2));

if(!sla) throw new Error("sla expected as parameter --sla=x");
if(!parallelProcessingCapacity) logger.warn("parallelProcessingCapacity expected as parameter --parallelProcessingCapacity=x, default set to 10");
if(!privateKey) logger.warn("privateKey expected as parameter --privateKey=\"/path/to\", default set to \"/home/ec2-user/aws-scraper-cost-optimization/local/aws-scraper-cost-optimization.pem\"");
if(!resultsPath) logger.warn("resultsPath expected as parameter --resultsPath=\"/path/to\", default set to \"/home/ec2-user/aws-scraper-cost-optimization/results\"");
if(!maximumClusterSize) logger.warn("maximumClusterSize expected as parameter --maximumClusterSize=x, default set to 5");

instanceType = "t2.small";
parallelProcessingCapacity = 10;
privateKey = "/home/ec2-user/aws-scraper-cost-optimization/local/aws-scraper-cost-optimization.pem";
resultsPath = "/home/ec2-user/aws-scraper-cost-optimization/results";
maximumClusterSize = 5;

let currentCost = 0;
let currentIteration = 0;

const startTime = Date.now();

const clusterSizeRecords = []; // initial values will be added on first iteration
const creditBalanceRecords = []; // initial values will be added on first iteration
const cpuUsageRecords = []; // initial values will be added on first iteration
const processingTimeRecords = [[0, 0]];
const approximateNumberOfMessagesRecords = [[0, 0]];
const currentCostRecords = [[0, 0]];

const cronInterval = Math.ceil(sla / 4);
const cronIntervalInSeconds = Math.ceil(cronInterval / 1000);

logger.info(getLocalTime(), "OrchestrateInstances script initiated with parameters: ", { sla, instanceType, parallelProcessingCapacity, cronIntervalInSeconds });

const job = new CronJob(
  `0/${cronIntervalInSeconds} * * * * *`,
  async function () {
    const clusterInstances = await InstancesHelper.getInstances({
      filters: [
        {
          Name: "instance-state-name",
          Values: ["running", "pending"]
        },
      ],
    });

    const burstableInstanceId = clusterInstances.find((instance) => instance.InstanceType.includes("t3")).InstanceId;

    const currentCreditBalance = await CloudWatchHelper.getLastMetric({
      metricDataQuery: {
        Id: "cpuCreditBalance",
        MetricStat: {
          Metric: {
            Dimensions: [
              {
                Name: "InstanceId",
                Value: burstableInstanceId
              },
            ],
            MetricName: "CPUCreditBalance",
            Namespace: "AWS/EC2"
          },
          Period: 60,
          Stat: "Maximum",
        },
      }
    });

    const currentCPUUtilization = await CloudWatchHelper.getLastMetric({
      metricDataQuery: {
        Id: "cpuUtilization",
        MetricStat: {
          Metric: {
            Dimensions: [
              {
                Name: "InstanceId",
                Value: burstableInstanceId
              },
            ],
            MetricName: "CPUUtilization",
            Namespace: "AWS/EC2"
          },
          Period: 60,
          Stat: "Maximum",
        },
      }
    });

    if (currentIteration > 0) {
      const activeInstanceTypes = clusterInstances.map((instance) => instance.InstanceType);

      for (const activeInstanceType of activeInstanceTypes) currentCost += (ec2Pricing[activeInstanceType] / 3600) * cronIntervalInSeconds;
    } else {
      clusterSizeRecords.push([clusterInstances.length, 0]);
      creditBalanceRecords.push([currentCreditBalance, 0]);
      cpuUsageRecords.push([currentCPUUtilization, 0]);
    }

    const approximateNumberOfMessages = await getApproximateNumberOfMessages();

    let {
      averageClusterServiceTime,
      averageClusterProcessingTime,
    } = await getClusterMetrics({ startTime });

    // 30 sec, default value if system recently started running
    averageClusterServiceTime = averageClusterServiceTime || 20000;
    const queueName = config.get("AWS").SQS_QUEUE_URL.split("/").pop();

    const approximateAgeOfOldestMessageInSeconds = await CloudWatchHelper.getLastMetric({
      metricDataQuery: {
        Id: "approximateAgeOfOldestMessage",
        MetricStat: {
          Metric: {
            Dimensions: [
              {
                Name: "QueueName",
                Value: queueName
              },
            ],
            MetricName: "ApproximateAgeOfOldestMessage",
            Namespace: "AWS/SQS"
          },
          Period: 60,
          Stat: "Maximum",
        },
      }
    });

    const approximateAgeOfOldestMessage = approximateAgeOfOldestMessageInSeconds * 1000;

    logger.info(getLocalTime(),
      {
        currentIteration,
        currentCost,
        approximateNumberOfMessages,
        approximateAgeOfOldestMessage,
        averageClusterServiceTime,
        averageClusterProcessingTime,
        currentCreditBalance,
        currentCPUUtilization
      }
    );

    if(approximateNumberOfMessages) {
      const idealClusterSize = Math.ceil((approximateNumberOfMessages * averageClusterServiceTime) / (sla * parallelProcessingCapacity));

      // const newClusterSize = Math.min(maximumClusterSize, idealClusterSize);
      const newClusterSize = 1;

      const actualClusterSize = clusterInstances.length;

      logger.info(getLocalTime(), { idealClusterSize, actualClusterSize, newClusterSize });

      if(newClusterSize > actualClusterSize) {
        const newInstances = await InstancesHelper.createInstances({
          numberOfInstances: newClusterSize - actualClusterSize,
          instanceType
        });

        const startCrawlPromises = newInstances.map(
          async ({ instanceId }) => {
            const instanceStatus = await InstancesHelper.waitInstanceFinalStatus({ instanceId });

            if(instanceStatus === "running") {
              await sleep(40000); // 40 sec, wait after status changes to running

              await InstancesHelper.startQueueConsumeOnInstance({ instanceId, privateKey, readBatchSize: parallelProcessingCapacity, clouwatchLogGroupName: config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME, sqsQueueUrl: config.get("AWS").SQS_QUEUE_URL, s3ResultBucketName: config.get("AWS").S3_RESULT_BUCKET_NAME });
            } else {
              logger.warn(getLocalTime(), "Instance failed creation", { instanceStatus });
            }
          }
        );

        Promise.all(startCrawlPromises);

      } else if (newClusterSize < actualClusterSize) {
        if (approximateAgeOfOldestMessage < sla) {
          await InstancesHelper.terminateInstances({ numberOfInstances: actualClusterSize - newClusterSize });
        } else {
          logger.warn(getLocalTime(), "Will not reduce cluster because oldest message is greater then SLA", { approximateAgeOfOldestMessage, sla });
        }
      }

      const currentTimestamp = (currentIteration + 1) * cronIntervalInSeconds;

      // Update metric records
      clusterSizeRecords.push([actualClusterSize, currentTimestamp]);
      creditBalanceRecords.push([currentCreditBalance, currentTimestamp]);
      processingTimeRecords.push([averageClusterProcessingTime / 1000, currentTimestamp]);
      approximateNumberOfMessagesRecords.push([approximateNumberOfMessages, currentTimestamp]);
      cpuUsageRecords.push([currentCPUUtilization, currentTimestamp]);
      currentCostRecords.push([currentCost, currentTimestamp]);

      currentIteration += 1;
    } else {
      const resultLabel = new Date().toLocaleString("en-US", { timeZone: "America/Sao_Paulo" }).replace(/[^0-9]/g, "");

      // const clusterAverageProcessingTimeVariance = processingTimeRecords.reduce(
      //   (accumulator, [processingTime]) => processingTime > 0
      //                                       ? accumulator + ((processingTime - (averageClusterProcessingTime / 1000)) ** 2)
      //                                       : accumulator,
      //   0,
      // ) / processingTimeRecords.length;

      // const clusterMessageProcessingTimeStandardDeviation = Math.sqrt(clusterAverageProcessingTimeVariance);

      let data = "id;time;cost;clusterSize;processingTimeRecords;approximateNumberOfMessagesRecords;creditBalanceRecords;cpuUsageRecords\n";

      for(let i = 0; i < currentIteration; i++) {
        data += `"${resultLabel}"`;
        data += `;${i * (sla / 4) / 1000}`;
        data += `;${currentCostRecords[i][0]}`;
        data += `;${clusterSizeRecords[i][0]}`;
        data += `;${processingTimeRecords[i][0]}`;
        data += `;${approximateNumberOfMessagesRecords[i][0]}`;
        data += `;${creditBalanceRecords[i][0]}`;
        data += `;${cpuUsageRecords[i][0]}`;
        // Deviation
        data += "\n";
      }

      fs.writeFileSync(`${resultsPath}/execution_data_${resultLabel}.csv`, data);
      
      this.stop();
    }

    return true;
  }
);

job.start();
