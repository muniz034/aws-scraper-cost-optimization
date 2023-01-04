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

async function processMessages(messages, browser, S3_RESULT_BUCKET_NAME) {
  const messagesBodies = messages.map(
      ({ Body, ReceiptHandle, Attributes: { ApproximateFirstReceiveTimestamp } }) =>
          ({ tags: { ReceiptHandle }, firstReceivedAt: ApproximateFirstReceiveTimestamp, ...JSON.parse(Body) })
  );

  const results = await startScrapingBatch({ entries: messagesBodies }, browser, S3_RESULT_BUCKET_NAME);

  const receiptsToDelete = [];

  for(const result of results){
    const { ReceiptHandle } = result.tags;

    if(result.success){
      receiptsToDelete.push(ReceiptHandle);
    } else {
      logger.error(getLocalTime(), "A Scraping Request has failed -- Closing browser");
    }
  }

  if(receiptsToDelete.length != results.length) await browser.close();

  const processedResults = results.filter((result) => result.success);

  if(processedResults.length == 0) return null;

  const totalMessageProcessingTime = processedResults.reduce((total, { processingTime }) => total + processingTime, 0);
  const totalMessageServiceTime = processedResults.reduce((total, { serviceTime }) => total + serviceTime, 0);

  const averageMessageProcessingTimeOnBatch = totalMessageProcessingTime / processedResults.length;
  const averageMessageServiceTimeOnBatch = totalMessageServiceTime / processedResults.length;

  const totalMessageProcessingTimeDeviation = processedResults.reduce((total, { processingTime }) => total + ((processingTime - averageMessageProcessingTimeOnBatch) ** 2), 0);

  const messageProcessingTimeVarianceOnBatch = totalMessageProcessingTimeDeviation / processedResults.length;

  const messageProcessingTimeStandardDeviationOnBatch = Math.sqrt(messageProcessingTimeVarianceOnBatch);

  return { receiptsToDelete, averageMessageProcessingTimeOnBatch, averageMessageServiceTimeOnBatch, messageProcessingTimeStandardDeviationOnBatch };
};

logger.setLevel("info");

const sqs = new SQSClient({ region: "us-east-1" });

if(isMainThread){
  let {
    _,
    readBatchSize,
    sqsQueueUrl,
    s3ResultBucketName,
    clouwatchLogGroupName
  } = minimist(process.argv.slice(2));

  if(!readBatchSize) throw new Error("readBatchSize expected as parameter --readBatchSize=x");
  if(readBatchSize > 10) logger.warn(getLocalTime(), `readBatchSize expected to be less than 10, will be limited to 10`, { readBatchSize });
  
  const queue = new PQueue({concurrency: 1});
  const instanceId = await getInstanceId();
  const cloudWatchHelper = new CloudWatchHelper(clouwatchLogGroupName ?? config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME);
  const logStreamName = `consume-queue-execution_${Date.now()}_${instanceId}`;

  let processedBatches = 0;
  let averageMessageProcessingTimeAccumulator = 0;
  let messageProcessingTimeStandardDeviationAccumulator = 0;
  let averageMessageServiceTimeAccumulator = 0;

  await cloudWatchHelper.initializeLogStream(logStreamName);

  logger.info(getLocalTime(), "ConsumeQueue script initiated with parameters ", { readBatchSize, logStreamName });

  // ==================== Cria threads e seta os eventListeners ======================== //
  const threadCount = os.cpus().length;
  const threads = new Set();

  for(let i = 0; i < threadCount; i++) threads.add(new Worker("./src/functions/scripts/consumeQueue.js", { workerData: { id: i, readBatchSize, sqsQueueUrl, s3ResultBucketName, clouwatchLogGroupName} }));

  for(let worker of threads) {
    worker.on("error", (err) => { 
      throw err; 
    });

    worker.on("exit", async () => {
      threads.delete(worker);
    });

    worker.on("message", async (msg) => {
      const {
        averageMessageProcessingTimeOnBatch,
        averageMessageServiceTimeOnBatch,
        messageProcessingTimeStandardDeviationOnBatch,
      } = msg;

      await queue.add(async () => {
        messageProcessingTimeStandardDeviationAccumulator += messageProcessingTimeStandardDeviationOnBatch ** 2;
        averageMessageProcessingTimeAccumulator += averageMessageProcessingTimeOnBatch;
        averageMessageServiceTimeAccumulator += averageMessageServiceTimeOnBatch;
  
        processedBatches += 1;
  
        const averageMessageProcessingTime = averageMessageProcessingTimeAccumulator / processedBatches;
        const averageMessageServiceTime = averageMessageServiceTimeAccumulator / processedBatches;
        // https://stats.stackexchange.com/questions/25848/how-to-sum-a-standard-deviation
        const executionProcessingTimeStandardDeviation = Math.sqrt((messageProcessingTimeStandardDeviationAccumulator)/processedBatches);

        logger.info(getLocalTime(), `[MASTER] Logging execution metrics`, {
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
      });
    });
  }
  // =================================================================================== //
} else {
  // ============ Inicia processo de Requisição de Mensagens e Scraping ================ //
  let browser = await launchBrowser();
  let counter = 0; // Contador com finalidade de "resetar" o browser, para evitar memory leak
  const { id, readBatchSize, sqsQueueUrl, s3ResultBucketName, clouwatchLogGroupName } = workerData;
  const tryAgainDelay = 1000;

  const SQS_QUEUE_URL = sqsQueueUrl ?? config.get("AWS").SQS_QUEUE_URL;
  const S3_RESULT_BUCKET_NAME = s3ResultBucketName ?? config.get("AWS").S3_RESULT_BUCKET_NAME;
  const CLOUDWATCH_LOG_GROUP_NAME = clouwatchLogGroupName ?? config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME;

  const instanceId = await getInstanceId();
  const cloudWatchHelper = new CloudWatchHelper(CLOUDWATCH_LOG_GROUP_NAME);
  const logStreamName = `consume-queue-execution_${Date.now()}_${instanceId}_thread${id}`;

  await cloudWatchHelper.initializeLogStream(logStreamName);

  while(true) {
    if(!browser.isConnected()){
      logger.info(getLocalTime(), `[${id}] Browser was closed, opening another one`);
      browser = await launchBrowser();
    } else if(counter > 5) {
      logger.info(getLocalTime(), `[${id}] Retrieved +${counter} batches, restarting the browser`);
      await browser.close();
      browser = await launchBrowser();
      counter = 0;
    }

    let Messages = [];
    let numberOfReads = 0;
    
    // Ler mensagens
    while(Messages.length < readBatchSize && numberOfReads < 3) {
      const params = {
        QueueUrl: SQS_QUEUE_URL,
        MaxNumberOfMessages: readBatchSize < 10 ? readBatchSize : 10,
        AttributeNames: ["ApproximateFirstReceiveTimestamp"] // if is smaller then maximum use it, else use SQS maximum per read (10)
      };
  
      const { Messages: NewMessages = [] } = await sqs.send(new ReceiveMessageCommand(params));
  
      Messages = Messages.concat(NewMessages);
      numberOfReads += 1;
    };
  
    if(Messages.length) {
      logger.info(getLocalTime(), `[${id}] Initiating processMessages script`, { messagesNumber: Messages.length });

      let result;

      try {
        result = await processMessages(Messages, browser, S3_RESULT_BUCKET_NAME);
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

      parentPort.postMessage({
        averageMessageProcessingTimeOnBatch,
        averageMessageServiceTimeOnBatch,
        messageProcessingTimeStandardDeviationOnBatch
      });

      // logger.info(getLocalTime(), `[${id}] Logging batch metrics`, { averageMessageProcessingTimeOnBatch, averageMessageServiceTimeOnBatch, messageProcessingTimeStandardDeviationOnBatch });

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

      counter += 1;
  
    } else {
      // logger.info(getLocalTime(), `[${id}] Queue is empty, waiting to try again`, { tryAgainDelay });
      // await browser.close();
      // break;
    }
  }
  // =================================================================================== //
}