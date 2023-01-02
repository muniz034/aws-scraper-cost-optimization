import logger from "loglevel";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import minimist from "minimist";
import { v4 as uuidv4 } from "uuid";
import config from "config";
import * as fs from "fs";

import sleep from "../../utils/sleep.js";
import getLocalTime from "../../utils/getLocalTime.js";

class PoissonProcess {
  // averageTimeBetweenEvents - Tempo Médio entre Eventos
  // maxNumberOfObservableEvents - Número Máximo de Ocorrência de Eventos
  // maxExecutionTime - Tempo Máximo de Execução
  // lambda - Número Médio de Ocorrências por Segundo
  constructor(averageTimeBetweenEvents, { maxNumberOfObservableEvents, maxExecutionTime }){
      this._averageTimeBetweenEvents = averageTimeBetweenEvents / 1e3;
      this._lambda = 1/averageTimeBetweenEvents;
      this._maxNumberOfObservableEvents = maxNumberOfObservableEvents;
      this._maxExecutionTime = maxExecutionTime;
  }

  // https://github.com/python/cpython/blob/797edb28c3dd02a5727f0374e937e906a389ab77/Lib/random.py#L580
  _expovariate(lambda){
      return -Math.log(1.0 - Math.random())/lambda;
  }

  // __calculate_inter_arrival_time_in_seconds
  _calculateInterintervalTime(lambda){
      return this._expovariate(lambda);
  }

  // __print_lambda_rate
  printLambdaRate(){
      return console.log(`Lambda Rate λ (Average Number of Events per Second): 1/${this._averageTimeBetweenEvents} = ${this._lambda}`);
  }

  // __generate_arrival_times_lists_in_seconds
  generateArrivalTimesList(){
      let arrivalTime = 0;
      let interArrivalTimeList = [];
      let arrivalTimeList = [];

      if(this._maxExecutionTime){
          while(arrivalTime <= this._maxExecutionTime){
              let interArrivalTime = this._calculateInterintervalTime(this._lambda);
              interArrivalTimeList.push(interArrivalTime);
              arrivalTime += interArrivalTime;
              arrivalTimeList.push(arrivalTime);
          }
      } else {
          while(interArrivalTimeList.length != this._maxNumberOfObservableEvents){
              let interArrivalTime = this._calculateInterintervalTime(this._lambda);
              interArrivalTimeList.push(interArrivalTime);
              arrivalTime += interArrivalTime;
              arrivalTimeList.push(arrivalTime);
          }
      }

      return [interArrivalTimeList, arrivalTimeList];
  }
}

function arrivalTimeListToCSV(filename, id, arrivalTimeList, batchSize) {
  let totalMessages = 0;
  let data = `id;time;totalMessages`;

  for(let time of arrivalTimeList){
    totalMessages += batchSize;
    data += `\n"${id}";${time};${totalMessages}`
  }

  fs.writeFileSync(filename, data);
}

async function sendMessage(message) {
  const uniqueId = uuidv4();

  const params = {
    MessageBody: JSON.stringify({ ...message, createdAt: Date.now() }),
    QueueUrl: config.get("AWS").SQS_QUEUE_URL,
    MessageGroupId: uniqueId, // messages that belong to the same message group are processed in a FIFO manner (however, messages in different message groups might be processed out of order)
    MessageDeduplicationId: uniqueId
  };

  return sqs.send(new SendMessageCommand(params));
}

const sqs = new SQSClient({ region: "us-east-1" });

logger.setLevel("info");

let {
  _,
  batchSize,
  numberOfBatches,
  delay
} = minimist(process.argv.slice(2));

if(!batchSize) throw new Error("batchSize expected as parameter --batchSize=x");
if(!numberOfBatches) throw new Error("numberOfBatches expected as parameter --numberOfBatches=x");
if(!delay) throw new Error("delay expected as parameter --delay=x");

const defaultMessage = {
  type: "esaj",
  name: "sp",
  informations: {
    cpf: "11111"
  }
};

const poisson = new PoissonProcess(delay, { maxNumberOfObservableEvents: numberOfBatches });
const id = new Date().toLocaleString("en-US", { timeZone: "America/Sao_Paulo" }).replace(/[^0-9]/g, "");;

const [
  interArrivalTimeList,
  arrivalTimeList
] = [...poisson.generateArrivalTimesList()];

logger.info(getLocalTime(), "PopulateQueue script initiated with parameters: ", { ...defaultMessage, batchSize, numberOfBatches, delay });
logger.info(getLocalTime(), "Saving arrivalTimeList to: ", { file: `./populate_data_${id}.csv` });

arrivalTimeListToCSV(`./results/populate_data_${id}.csv`, id, arrivalTimeList, batchSize);

let index = 1;

for(let time of interArrivalTimeList){
  logger.info(getLocalTime(), { delay: time });

  if(index != numberOfBatches) await sleep(time);

  logger.info(getLocalTime(), "Sending batch", { batchNumber: index });

  const messages = Array.from({ length: batchSize }, (_, index) => ({ body: defaultMessage, deduplicationId: index.toString() }));

  const promises = messages.map((message) => sendMessage(message.body));

  await Promise.all(promises);
  
  index += 1;
}
