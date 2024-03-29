import metadata from "node-ec2-metadata";
import config from "config";

export default async function getInstanceId(IS_INSTANCE) {
  let instanceId;

  if(IS_INSTANCE){
    instanceId = await metadata.getMetadataForInstance("instance-id");
  } else if(config.get("Enviroment").IS_INSTANCE) {
    instanceId = await metadata.getMetadataForInstance("instance-id");
  } else {
    instanceId = config.get("Enviroment").IS_LOCAL ? "local" : "lambda";
  }

  return instanceId;
}