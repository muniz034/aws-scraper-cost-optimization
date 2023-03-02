import logger from "loglevel";
import { SQSClient, GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import minimist from "minimist";
import { CronJob } from "cron";
import config from "config";

import sleep from "../../utils/sleep.js";
import ec2Pricing from "../../constants/ec2Pricing.js";
import InstancesHelper from "../../helpers/instancesHelper.js";
import CloudWatchHelper from "../../helpers/cloudWatchHelper.js";
import getLocalTime from "../../utils/getLocalTime.js";
import getInstanceId from "../../utils/getInstanceId.js";

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

const sqs = new SQSClient({ region: "us-west-1" });

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
  maximumClusterSize,
  creditLimit
} = minimist(process.argv.slice(2));

let isBurstable = "true";
let isOldStrategy = "false";

if(!sla) throw new Error(`${getLocalTime()} sla expected as parameter --sla=x`);
if(!creditLimit && isBurstable == "true") throw new Error("creditLimit expected as parameter --creditLimit=x");
if(!parallelProcessingCapacity) logger.warn(getLocalTime(), "parallelProcessingCapacity expected as parameter --parallelProcessingCapacity=x, default set to 5");
if(!privateKey) logger.warn(getLocalTime(), "privateKey expected as parameter --privateKey=\"/path/to\", default set to \"/home/ec2-user/aws-scraper-cost-optimization/local/pedro_key.pem\"");
if(!maximumClusterSize) logger.warn(getLocalTime(), "maximumClusterSize expected as parameter --maximumClusterSize=x, default set to 10");

instanceType = isBurstable === "true" ? "t3.micro" : "m1.small";
parallelProcessingCapacity = 5;
privateKey = "/home/ec2-user/aws-scraper-cost-optimization/local/pedro_key.pem";
maximumClusterSize = 10;

let currentCost = 0;
let currentIteration = 0;

const startTime = Date.now();

const cronInterval = Math.ceil(sla / 4);
const cronIntervalInSeconds = Math.ceil(cronInterval / 1000);

const instanceId = await getInstanceId();
const cloudWatchHelper = new CloudWatchHelper(config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME);
const logStreamName = `orchestrate-instances-execution_${Date.now()}_${instanceId}`;
await cloudWatchHelper.initializeLogStream(logStreamName);

logger.info(getLocalTime(), "OrchestrateInstances script initiated with parameters: ", { sla, instanceType, parallelProcessingCapacity, cronIntervalInSeconds, isBurstable });

const job = new CronJob(
  `0/${cronIntervalInSeconds} * * * * *`,
  async function () {
    const clusterInstances = await InstancesHelper.getInstances({
      filters: [
        {
          Name: "instance-state-name",
          Values: ["running", "pending"]
        },
        {
          Name: "instance-type",
          Values: [instanceType]
        },
        {
          Name: "tag:from",
          Values: [instanceId]
        }
      ],
    });

    // Buscar instancias em estado de ACCRUE no ciclo atual
    const accrueInstances = await InstancesHelper.getInstances({
      filters: [
        {
          Name: "instance-state-name",
          Values: ["running", "pending"]
        },
        {
          Name: "tag:frameworkState",
          Values: ["accrue"]
        },
        {
          Name: "instance-type",
          Values: [instanceType]
        },
        {
          Name: "tag:from",
          Values: [instanceId]
        }
      ],
    });

    // Buscar instancias em estado de SPEND no ciclo atual
    const spendInstances = await InstancesHelper.getInstances({
      filters: [        
        {
          Name: "instance-state-name",
          Values: ["running", "pending"]
        },
        {
          Name: "tag:frameworkState",
          Values: ["spend"]
        },
        {
          Name: "instance-type",
          Values: [instanceType]
        },
        {
          Name: "tag:from",
          Values: [instanceId]
        }
      ],
    });

    if (currentIteration > 0) {
      const activeInstanceTypes = clusterInstances.map((instance) => instance.InstanceType);
      for (const activeInstanceType of activeInstanceTypes) currentCost += (ec2Pricing[activeInstanceType] / 3600) * cronIntervalInSeconds;
    }

    const approximateNumberOfMessages = await getApproximateNumberOfMessages();

    let {
      averageClusterServiceTime,
      averageClusterProcessingTime,
    } = await getClusterMetrics({ startTime });

    // 30 sec, default value if system recently started running
    averageClusterProcessingTime = averageClusterProcessingTime ?? 0;
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
        averageClusterProcessingTime
      }
    );

    const idealClusterSize = Math.ceil((approximateNumberOfMessages * averageClusterServiceTime) / (sla * parallelProcessingCapacity));

    const actualClusterSize = clusterInstances.length;

    const newClusterSize = idealClusterSize > 0 ? Math.min(maximumClusterSize, idealClusterSize) : actualClusterSize;

    logger.info(getLocalTime(), { idealClusterSize, actualClusterSize, newClusterSize });
    
    await cloudWatchHelper.logAndRegisterMessage(
      JSON.stringify(
        {
          message: "Current orchestrate metrics",
          type: "orchestrateMetrics",
          isBurstable,
          instanceId,
          approximateNumberOfMessages,
          averageClusterProcessingTime: averageClusterProcessingTime / 1000,
          averageClusterServiceTime,
          newClusterSize,
          actualClusterSize,
          accrueInstances: accrueInstances.length,
          spendInstances: spendInstances.length,
          currentCost
        }
      ),
    );

    if(newClusterSize < (accrueInstances.length + spendInstances.length)){
      if((accrueInstances.length > spendInstances.length || accrueInstances.length == 0) && approximateAgeOfOldestMessage < sla){
        await InstancesHelper.terminateInstances({ 
          numberOfInstances: (accrueInstances.length + spendInstances.length) - newClusterSize, 
          instanceCreator: instanceId,
          isOldStrategy, 
          isBurstable
        });

      } else {
        logger.warn(getLocalTime(), "Will not reduce cluster because oldest message is greater then SLA", { approximateAgeOfOldestMessage, sla });
      }
    } else if(newClusterSize > (accrueInstances.length + spendInstances.length)) {
      const newInstances = await InstancesHelper.createInstances({
        numberOfInstances: newClusterSize - spendInstances.length,
        instanceType,
        instanceCreator: instanceId,
        isOldStrategy,
        isBurstable
      });

      const startCrawlPromises = newInstances.map(
        async ({ instanceId }) => {
          const instanceStatus = await InstancesHelper.waitInstanceFinalStatus({ instanceId });

          if(instanceStatus === "running") {
            logger.info(getLocalTime(), "Waiting 40s to continue...");
            await sleep(40000); // 40 sec, wait after status changes to running

            try {
              await InstancesHelper.startQueueConsumeOnInstance({ instanceId, creditLimit, isBurstable, privateKey, readBatchSize: parallelProcessingCapacity, clouwatchLogGroupName: config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME, sqsQueueUrl: config.get("AWS").SQS_QUEUE_URL, s3ResultBucketName: config.get("AWS").S3_RESULT_BUCKET_NAME });
            } catch(error) {
              logger.info(getLocalTime(), `[${instanceId}]`, error);
            }

          } else {
            logger.warn(getLocalTime(), "Instance failed creation", { instanceStatus });
          }
        }
      );

      Promise.all(startCrawlPromises);
    }
    
    currentIteration += 1;
    return true;
    
  }
);

job.start();
