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
import InstancesHelper from "../../helpers/instancesHelper.js";
import sleep from "../../utils/sleep.js";

async function processMessages(messages, browser, S3_RESULT_BUCKET_NAME) {
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

  const isChromiumWorking = await validateChromium();

  if(!isChromiumWorking) throw new Error("It was not possible to execute Chromium");
  if(!readBatchSize) throw new Error("readBatchSize expected as parameter --readBatchSize=x");
  if(readBatchSize > 10) logger.warn(getLocalTime(), `readBatchSize expected to be less than 10, will be limited to 10`, { readBatchSize });
  
  const queue = new PQueue({concurrency: 1});
  const instanceId = await getInstanceId(sqsQueueUrl ? true : false); // Usa o parametro sqsQueueUrl pq se ele é passado, então todos os outros são (caso instancia seja criada pelo orquestrador)
  const cloudWatchHelper = new CloudWatchHelper(clouwatchLogGroupName ?? config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME);
  const logStreamName = `consume-queue-execution_${Date.now()}_${instanceId}`;

  let processedBatches = 0;
  let averageMessageProcessingTimeAccumulator = 0;
  let messageProcessingTimeStandardDeviationAccumulator = 0;
  let averageMessageServiceTimeAccumulator = 0;

  let credits = await CloudWatchHelper.getLastMetric({
    metricDataQuery: {
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
  });

  let state = credits < 4.8 ? "accrue" : "spend";

  await InstancesHelper.setTag({ instanceId, tag: "frameworkState", value: state });

  await cloudWatchHelper.initializeLogStream(logStreamName);

  logger.info(getLocalTime(), "[MASTER] ConsumeQueue script initiated with parameters ", { readBatchSize, logStreamName });

  // ==================== Cria threads e seta os eventListeners ======================== //
  const threadCount = os.cpus().length;
  const threads = new Set();
  const batchPerThread = Math.floor(readBatchSize / threadCount);

  for(let i = 0; i < threadCount; i++){
    let batchSize = i < (readBatchSize % threadCount) ? batchPerThread + 1 : batchPerThread;
    threads.add(new Worker("./src/functions/scripts/consumeQueue.js", { workerData: { id: i, instanceId, readBatchSize: batchSize, sqsQueueUrl, s3ResultBucketName, clouwatchLogGroupName} }));
  }

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
        credits
      } = msg;

      await queue.add(async () => {
        if(!credits){ // Caso não esteja apenas logando os créditos
          messageProcessingTimeStandardDeviationAccumulator += messageProcessingTimeStandardDeviationOnBatch ** 2;
          averageMessageProcessingTimeAccumulator += averageMessageProcessingTimeOnBatch;
          averageMessageServiceTimeAccumulator += averageMessageServiceTimeOnBatch;
    
          processedBatches += 1;
        }
  
        const averageMessageProcessingTime = averageMessageProcessingTimeAccumulator / processedBatches;
        const averageMessageServiceTime = averageMessageServiceTimeAccumulator / processedBatches;
        // https://stats.stackexchange.com/questions/25848/how-to-sum-a-standard-deviation
        const standardDeviationMessage = Math.sqrt((messageProcessingTimeStandardDeviationAccumulator)/processedBatches);
        const loggingCredits = credits ?? await CloudWatchHelper.getCredits({ instanceId });

        logger.info(getLocalTime(), `[MASTER] Logging execution metrics`, {
          processedBatches,
          // averageMessageProcessingTimeAccumulator,
          averageMessageProcessingTime,
          averageMessageServiceTime,
          standardDeviationMessage,
          loggingCredits
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
              standardDeviationMessage,
              loggingCredits
            }
          ),
        );

        state = loggingCredits < 4.8 ? "accrue" : "spend";
        
        if(loggingCredits < 4.8) await InstancesHelper.setTag({ instanceId, tag: "frameworkState", value: state });

      });
    });
  }
  // =================================================================================== //
} else {
  // ============ Inicia processo de Requisição de Mensagens e Scraping ================ //
  const { id, readBatchSize, sqsQueueUrl, s3ResultBucketName, clouwatchLogGroupName, instanceId } = workerData;
  const noMessageDelay = 15000;
  const stateDelay = 60000;

  const SQS_QUEUE_URL = sqsQueueUrl ?? config.get("AWS").SQS_QUEUE_URL;
  const S3_RESULT_BUCKET_NAME = s3ResultBucketName ?? config.get("AWS").S3_RESULT_BUCKET_NAME;
  const CLOUDWATCH_LOG_GROUP_NAME = clouwatchLogGroupName ?? config.get("AWS").CLOUDWATCH_LOG_GROUP_NAME;

  const cloudWatchHelper = new CloudWatchHelper(CLOUDWATCH_LOG_GROUP_NAME);

  let browser = await launchBrowser();
  let memoryLeakCounter = 0; // Contador com finalidade de "resetar" o browser, para evitar memory leak
  let state = await InstancesHelper.getTag({ instanceId, tag: "frameworkState" });
  let isFirstCheck = true;
  let lastCreditCheckTimestamp = Date.now();

  await cloudWatchHelper.initializeLogStream(`consume-queue-execution_${Date.now()}_${instanceId}_thread${id}`);

  while(true) {
    state = await InstancesHelper.getTag({ instanceId, tag: "frameworkState" });
    let isLastCheckFiveMinutesAgo = (Date.now() - lastCreditCheckTimestamp) > (5 * 60 * 1000);

    if(id == 0 && (isLastCheckFiveMinutesAgo || isFirstCheck)){
      let credits = await CloudWatchHelper.getCredits({ instanceId });

      state = credits < 4.8 ? "accrue" : "spend";

      await InstancesHelper.setTag({ instanceId, tag: "frameworkState", value: state });

      isFirstCheck = false;
      lastCreditCheckTimestamp = Date.now();

      logger.info(getLocalTime(), `[${id}] CPU Credits Balance`, { credits, state, lastCreditCheck: new Date(lastCreditCheckTimestamp) });
      
      parentPort.postMessage({
        credits
      });
    }

    if(state === "spend"){
      let { Messages: Messages = [] } = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: SQS_QUEUE_URL,
        MaxNumberOfMessages: readBatchSize < 10 ? readBatchSize : 10,
        AttributeNames: ["ApproximateFirstReceiveTimestamp"]
      }));
    
      if(Messages.length) {
        if(!browser.isConnected()){
          logger.info(getLocalTime(), `[${id}] Browser was closed, opening another one`);
          browser = await launchBrowser();
        } else if(memoryLeakCounter > 5) {
          logger.info(getLocalTime(), `[${id}] Retrieved ${memoryLeakCounter} batches, restarting the browser`);
          await browser.close();
          browser = await launchBrowser();
          memoryLeakCounter = 0;
        }

        logger.info(getLocalTime(), `[${id}] Initiating processMessages script`, { messagesNumber: Messages.length, isLastCheckFiveMinutesAgo });

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

        memoryLeakCounter += 1;
    
      } else {
        logger.info(getLocalTime(), `[${id}] Queue is empty, waiting to try again`, { noMessageDelay });
        if(browser.isConnected()) await browser.close();
        await sleep(noMessageDelay);
      }

      state = await InstancesHelper.getTag({ instanceId, tag: "frameworkState" });
    } else {
      logger.info(getLocalTime(), `[${id}] Instance is in ACCRUE state, waiting to try again`, { stateDelay });
      if(browser.isConnected()) await browser.close();
      await sleep(stateDelay);
    }

  }
  // =================================================================================== //
}