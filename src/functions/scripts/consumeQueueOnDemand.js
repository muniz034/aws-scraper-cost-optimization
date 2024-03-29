import logger from "loglevel";
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { parentPort, Worker, isMainThread, workerData } from "worker_threads";
import PQueue from "p-queue";
import minimist from "minimist";
import config from "config";
import os from "os";

import startScrapingBatch from "../startScrapingBatch.js";
import getInstanceId from "../../utils/getInstanceId.js";
import CloudWatchHelper from "../../helpers/cloudWatchHelper.js";
import getLocalTime from "../../utils/getLocalTime.js";
import launchBrowser from "../../utils/launchBrowser.js";
import validateChromium from "../../utils/validateChromium.js";
import sleep from "../../utils/sleep.js";

async function processMessages(messages, browser, S3_RESULT_BUCKET_NAME, cloudWatchHelper, instanceId) {
  const messagesBodies = messages.map(
      ({ Body, ReceiptHandle, Attributes: { ApproximateFirstReceiveTimestamp } }) =>
          ({ tags: { ReceiptHandle }, firstReceivedAt: ApproximateFirstReceiveTimestamp, ...JSON.parse(Body) })
  );

  const results = await startScrapingBatch({ entries: messagesBodies }, browser, S3_RESULT_BUCKET_NAME);
  const receiptsToDelete = [];
  let sampleFailedRequestInformation;

  for(const result of results){
    const { ReceiptHandle } = result.tags;

    if(result.success){
      receiptsToDelete.push(ReceiptHandle);
    } else {
      sampleFailedRequestInformation = result;
    }
  }

  if(receiptsToDelete.length != results.length){
    logger.error(getLocalTime(), "A Scraping Request has failed -- Closing browser");
    logger.info(getLocalTime(), "Sample", { sampleFailedRequestInformation });
    await browser.close();
  }

  const processedResults = results.filter((result) => result.success);

  if(processedResults.length == 0) return null;

  const totalMessageProcessingTime = processedResults.reduce((total, { processingTime }) => total + processingTime, 0);
  const totalMessageServiceTime = processedResults.reduce((total, { serviceTime }) => total + serviceTime, 0);

  const averageMessageProcessingTimeOnBatch = totalMessageProcessingTime / processedResults.length;
  const averageMessageServiceTimeOnBatch = totalMessageServiceTime / processedResults.length;

  const totalMessageProcessingTimeDeviation = processedResults.reduce((total, { processingTime }) => total + ((processingTime - averageMessageProcessingTimeOnBatch) ** 2), 0);

  const messageProcessingTimeVarianceOnBatch = totalMessageProcessingTimeDeviation / processedResults.length;

  const messageProcessingTimeStandardDeviationOnBatch = Math.sqrt(messageProcessingTimeVarianceOnBatch);

  await cloudWatchHelper.logAndRegisterMessage(
    JSON.stringify({
      message: "Messages successfully processed",
      type: "batchMetrics",
      instanceId,
      averageMessageProcessingTimeOnBatch,
      averageMessageServiceTimeOnBatch,
      messageProcessingTimeStandardDeviationOnBatch,
    })
  );

  return { receiptsToDelete, averageMessageProcessingTimeOnBatch, averageMessageServiceTimeOnBatch, messageProcessingTimeStandardDeviationOnBatch };
};

logger.setLevel("info");

const sqs = new SQSClient({ region: "us-east-1" });

let {
  _,
  readBatchSize,
  sqsQueueUrl,
  s3ResultBucketName,
  clouwatchLogGroupName
} = minimist(process.argv.slice(2));

const SQS_QUEUE_URL = sqsQueueUrl ?? config.get("AWS").SQS_QUEUE_URL;
const S3_RESULT_BUCKET_NAME = s3ResultBucketName ?? config.get("AWS").S3_RESULT_BUCKET_NAME;
const CLOUDWATCH_LOG_GROUP_NAME = clouwatchLogGroupName ?? config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME;

const isChromiumWorking = await validateChromium();

if(!isChromiumWorking) throw new Error("It was not possible to execute Chromium");
if(!readBatchSize) throw new Error("readBatchSize expected as parameter --readBatchSize=x");
if(readBatchSize > 10) logger.warn(getLocalTime(), `readBatchSize expected to be less than 10, will be limited to 10`, { readBatchSize });

const instanceId = await getInstanceId(sqsQueueUrl ? true : false); // Checa o parametro sqsQueueUrl pq se ele é passado, então é pq é instancia (pois foi criado pelo orquestrador)
const cloudWatchHelper = new CloudWatchHelper(CLOUDWATCH_LOG_GROUP_NAME);
const logStreamName = `consume-queue-execution_${Date.now()}_${instanceId}`;

const tryAgainDelay = 15000;

let processedBatches = 0;
let averageMessageProcessingTimeAccumulator = 0;
let messageProcessingTimeStandardDeviationAccumulator = 0;
let averageMessageServiceTimeAccumulator = 0;

await cloudWatchHelper.initializeLogStream(logStreamName);

logger.info(getLocalTime(), "ConsumeQueue script initiated with parameters ", { readBatchSize, logStreamName });

let browser = await launchBrowser();
let memoryLeakCounter = 0; // Contador com finalidade de "resetar" o browser, para evitar memory leak

await cloudWatchHelper.initializeLogStream(`consume-queue-execution_${Date.now()}_${instanceId}`);

while(true) {
  let { Messages: Messages = [] } = await sqs.send(new ReceiveMessageCommand({
    QueueUrl: SQS_QUEUE_URL,
    MaxNumberOfMessages: readBatchSize < 10 ? readBatchSize : 10,
    AttributeNames: ["ApproximateFirstReceiveTimestamp"]
  }));

  if(Messages.length) {
    if(!browser.isConnected()){
      logger.info(getLocalTime(), `Browser was closed, opening another one`);
      browser = await launchBrowser();
    } else if(memoryLeakCounter > 5) {
      logger.info(getLocalTime(), `Retrieved ${memoryLeakCounter} batches, restarting the browser`);
      await browser.close();
      browser = await launchBrowser();
      memoryLeakCounter = 0;
    }

    logger.info(getLocalTime(), `Initiating processMessages script`, { messagesNumber: Messages.length });

    let result;

    try {
      result = await processMessages(Messages, browser, S3_RESULT_BUCKET_NAME, cloudWatchHelper, instanceId);
    } catch(err) {
      continue;
    }

    if(!result) continue;
    
    const {
      receiptsToDelete,
      averageMessageProcessingTimeOnBatch,
      averageMessageServiceTimeOnBatch,
      messageProcessingTimeStandardDeviationOnBatch,
    } = result;

    // Deleta mensagens que foram de fato processadas
    const receiptsToDeletePromises = [];

    for(const ReceiptHandle of receiptsToDelete){
      receiptsToDeletePromises.push(sqs.send(new DeleteMessageCommand({ QueueUrl: SQS_QUEUE_URL, ReceiptHandle })));
    }

    await Promise.all(receiptsToDeletePromises);

    messageProcessingTimeStandardDeviationAccumulator += messageProcessingTimeStandardDeviationOnBatch ** 2;
    averageMessageProcessingTimeAccumulator += averageMessageProcessingTimeOnBatch;
    averageMessageServiceTimeAccumulator += averageMessageServiceTimeOnBatch;
    
    processedBatches += 1;
    
    const averageMessageProcessingTime = averageMessageProcessingTimeAccumulator / processedBatches;
    const averageMessageServiceTime = averageMessageServiceTimeAccumulator / processedBatches;
    // https://stats.stackexchange.com/questions/25848/how-to-sum-a-standard-deviation
    const executionProcessingTimeStandardDeviation = Math.sqrt((messageProcessingTimeStandardDeviationAccumulator)/processedBatches);
    
    logger.info(getLocalTime(), `Logging execution metrics`, {
      processedBatches,
      // averageMessageProcessingTimeAccumulator,
      averageMessageProcessingTime,
      averageMessageServiceTime,
      executionProcessingTimeStandardDeviation,
    });
    
    await cloudWatchHelper.logAndRegisterMessage(
      JSON.stringify(
        {
          message: "Current execution metrics",
          type: "instanceMetrics",
          instanceId,
          processedBatches,
          averageMessageProcessingTimeAccumulator,
          averageMessageProcessingTime,
          averageMessageServiceTime,
          executionProcessingTimeStandardDeviation,
        }
      ),
    );

    memoryLeakCounter += 1;

  } else {
    logger.info(getLocalTime(), `Queue is empty, waiting to try again`, { tryAgainDelay });
    if(browser.isConnected()) await browser.close();
    await sleep(tryAgainDelay);
  }
}