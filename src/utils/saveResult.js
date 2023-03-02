import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

export default async function saveResult (result, bucketName, { type, name }) {
  const s3 = new S3Client({ region: "us-west-1" });
  const resultS3Path = `${type}/${name}/${Date.now()}`;

  await s3.send(new PutObjectCommand({
    ACL: 'private',
    Bucket: bucketName,
    Key: resultS3Path,
    Body: result,
    ContentType: 'text/html',
  }));

  return `https://${bucketName}.s3.amazonaws.com/${resultS3Path}`;
};
