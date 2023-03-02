import logger from "loglevel";
import { EC2Client, DescribeTagsCommand, CreateTagsCommand, DescribeInstancesCommand, RunInstancesCommand, TerminateInstancesCommand, DescribeInstanceStatusCommand } from "@aws-sdk/client-ec2";
import { NodeSSH } from "node-ssh";
import config from "config";

import sleep from "../utils/sleep.js";
import getLocalTime from "../utils/getLocalTime.js";
import CloudWatchHelper from "./cloudWatchHelper.js";

const ec2 = new EC2Client({ region: "us-west-1" });

export default class InstancesHelper {
  static async getInstances({ maximumNumberOfInstances, filters }) {
    const params = {
      Filters: [{ Name: "tag:Owner", Values: ["Pedro Muniz"] }],
    };

    params.Filters = params.Filters.concat(filters); // Solução para não buscar instancias de outros usuários depois de mudar a infraestrutura pro AWS da UFF

    const {
      Reservations: reservations,
    } = await ec2.send(new DescribeInstancesCommand(params));

    const instances = reservations.reduce(
      (instancesAccumulator, { Instances: currentInstances = [] }) => {
        return [...instancesAccumulator, ...currentInstances];
      },
      [],
    );

    const slicedInstances = maximumNumberOfInstances && instances.length > maximumNumberOfInstances ? instances.slice(0, maximumNumberOfInstances) : instances;

    if(slicedInstances.length == 0) return [];

    logger.info(getLocalTime(), "Fetched instances", { values: params.Filters ? params.Filters[0].Values : undefined, totalResults: slicedInstances.length });

    return slicedInstances;
  }

  static async createInstances({ numberOfInstances = 1, instanceType, instanceCreator, isOldStrategy, isBurstable } = {}) {
    logger.info(getLocalTime(), "Creating instances", { numberOfInstances, instanceType });

    let params = {
      ImageId: config.get("AWS").EC2_AMI_ID,
      InstanceType: instanceType,
      MinCount: numberOfInstances, // maximum number of instances to launch. If you specify more instances than Amazon EC2 can launch in the target Availability Zone, Amazon EC2 launches the largest possible number of instances above MinCount
      MaxCount: numberOfInstances, // minimum number of instances to launch. If you specify a minimum that is more instances than Amazon EC2 can launch in the target Availability Zone, Amazon EC2 launches no instances.
      KeyName: config.get("AWS").EC2_KEY_PAIR_NAME,
      SecurityGroupIds: [config.get("AWS").EC2_SECURITY_GROUP_ID]
    }

    if(isOldStrategy == "true"){
      params.CreditSpecification = "unlimited";
      params.TagSpecifications = [{ 
        ResourceType: "instance", 
        Tags: [
          { Key: "isOrchestrator", Value: "false" }, 
          { Key: "from", Value: instanceCreator },
          { Key: "Owner", Value: "Pedro Muniz"}
        ]
      }];
    } else if(isBurstable == "true"){
      params.CreditSpecification = "standard";
      params.TagSpecifications = [{ 
        ResourceType: "instance", 
        Tags: [
          { Key: "isOrchestrator", Value: "false" }, 
          { Key: "frameworkState", Value: "accrue" }, 
          { Key: "from", Value: instanceCreator },
          { Key: "Owner", Value: "Pedro Muniz"}
        ]
      }];
    } else {
      params.TagSpecifications = [{ 
        ResourceType: "instance", 
        Tags: [
          { Key: "isOrchestrator", Value: "false" }, 
          { Key: "from", Value: instanceCreator },
          { Key: "Owner", Value: "Pedro Muniz"}
        ]
      }];
    }

    const { Instances: instances } = await ec2.send(new RunInstancesCommand(params));

    return instances.map(
      (instance) => (
        {
          imageId: instance.ImageId,
          instanceId: instance.InstanceId,
          state: instance.State?.Name
        }
      )
    );

  }

  static async terminateInstances({ numberOfInstances, instanceCreator, isBurstable, isOldStrategy } = {}) {
    let params = {
      filters: [
        {
          Name: "tag:isOrchestrator",
          Values: ["false"]
        },
        {
          Name: "instance-state-name",
          Values: ["running"]
        },
        {
          Name: "tag:from",
          Values: [instanceCreator]
        },
        {
          Name: "tag:Owner",
          Values: ["Pedro Muniz"]
        }
      ],
    };

    if(isBurstable == "true" && isOldStrategy == "false"){
      params.filters.push({ Name: "tag:frameworkState", Values: ["accrue"] });
      params.maximumNumberOfInstances = 10;
    } else {
      params.maximumNumberOfInstances = numberOfInstances;
    }

    const instances = await this.getInstances(params);

    let instanceIds = instances.map((instance) => instance.InstanceId);

    if(!instanceIds || instanceIds.length == 0){
      logger.warn(getLocalTime(), "Couldn't find instances to delete");

      return [];
    }

    if(isBurstable == "true" && isOldStrategy == "false"){
      let instanceIdsWithCredits = instanceIds.map(async (instance) => {
        return { id: instance, credits: await CloudWatchHelper.getCredits({instanceId: instance}) };
      });

      instanceIdsWithCredits.sort((a, b) => {
        if(a.credits > b.credits) return 1;
        if(a.credits < b.credits) return -1;
        return 0;
      });

      instanceIds = instanceIdsWithCredits.map((instance) => instance.id);

      instanceIds = instanceIds.slice(0, numberOfInstances);
    }
    
    logger.info(getLocalTime(), "Terminating instances", { instanceIds });

    const {
      TerminatingInstances: terminatingInstances,
    } = await ec2.send(new TerminateInstancesCommand({
      InstanceIds: instanceIds,
    }));

    return terminatingInstances.map(
      (terminatingInstance) => (
        {
          instanceId: terminatingInstance.InstanceId,
          newState: terminatingInstance.CurrentState?.Name,
          previousState: terminatingInstance.PreviousState?.Name,
        }
      )
    );
    
  }

  static async getInstanceStatus({ instanceId }) {
    const instances = await ec2.send(new DescribeInstanceStatusCommand({
      InstanceIds: [instanceId],
    }));

    const instanceExists = instances ?? false;

    if(instanceExists){
      let isPending = instances.InstanceStatuses.length == 0;
      if(isPending) return "unavailable";
      
      return instances.InstanceStatuses[0].InstanceState.Name;
    }

    return "unavailable";
  }

  static async waitInstanceFinalStatus({ instanceId }) {
    let status = await this.getInstanceStatus({ instanceId });

    logger.info(getLocalTime(), "Fetched initial instance status", { instanceStatus: status, instanceId });

    while (!["running", "shutting-down", "terminated", "stopped"].includes(status)) {
      logger.info(getLocalTime(), "Fetched non final instance status, waiting 10 seconds and trying again", { instanceStatus: status, instanceId });
      await sleep(10000);

      status = await this.getInstanceStatus({ instanceId });
    }

    logger.info(getLocalTime(), "Fetched final instance status", { instanceStatus: status, instanceId });
    return status;
  }

  static async startQueueConsumeOnInstance({
    instanceId,
    username = "ec2-user",
    privateKey = "/home/ec2-user/aws-scraper-cost-optimization/local/scraper-instance-key-pair.pem",
    readBatchSize = 5,
    sqsQueueUrl,
    s3ResultBucketName,
    clouwatchLogGroupName,
    isBurstable,
    creditLimit,
    isOldStrategy
  } = {}) {
    logger.info(getLocalTime(), "Getting public dns of the provided instance", { instanceId });

    const {
      Reservations: [{ Instances: [instance] } = {}]
    } = await ec2.send(new DescribeInstancesCommand({
      InstanceIds: [instanceId],
    }));

    const {
      PublicDnsName: host
    } = instance;

    const ssh = new NodeSSH();

    logger.info(getLocalTime(), "Connect SSH", { instanceId, username, privateKey });

    await ssh.connect({
      host,
      username,
      privateKey,
    });

    let params = "";

    if(isOldStrategy){
      params = [`npm run consumeQueueOld -- --readBatchSize=${readBatchSize} --sqsQueueUrl=${sqsQueueUrl} --s3ResultBucketName=${s3ResultBucketName} --clouwatchLogGroupName=${clouwatchLogGroupName}`];
    } else if(isBurstable == "true") {
      params = [`npm run consumeQueue -- --creditLimit=${creditLimit} --readBatchSize=${readBatchSize} --sqsQueueUrl=${sqsQueueUrl} --s3ResultBucketName=${s3ResultBucketName} --clouwatchLogGroupName=${clouwatchLogGroupName}`];
    } else {
      params = [`npm run consumeQueueOnDemand -- --readBatchSize=${readBatchSize} --sqsQueueUrl=${sqsQueueUrl} --s3ResultBucketName=${s3ResultBucketName} --clouwatchLogGroupName=${clouwatchLogGroupName}`];
    }

    params.push({ cwd:"/home/ec2-user/aws-scraper-cost-optimization" });
    
    // { cwd:"/home/ec2-user/aws-scraper-cost-optimization", onStdout(chunk) { console.log('stdoutChunk', chunk.toString('utf8')) }, onStderr(chunk) { console.log('stderrChunk', chunk.toString('utf8')) }}
    
    logger.info(getLocalTime(), "Run consume queue", { instanceId, username, privateKey, params });

    return ssh.execCommand(...params);
  }

  static async setTag({ instanceId, tag, value } = {}) {
    await ec2.send(new CreateTagsCommand({
      Resources: [instanceId],
      Tags: [{ Key: tag, Value: value }]
    }));
  }

  static async getTag({ instanceId, tag } = {}) {
    const { Tags } = await ec2.send(new DescribeTagsCommand({
      Filters: [
        {
          Name: "resource-type",
          Values: ["instance"] 
        },
        {
          Name: "resource-id",
          Values: [instanceId],
        },
        {
          Name: "key",
          Values: [tag]
        }
      ]
    }));

    return Tags[0]?.Value;
  }
}