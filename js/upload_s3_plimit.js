import fs from "fs";
import path from "path";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import pLimit from "p-limit";
import dotenv from "dotenv";

dotenv.config();

const BUCKET_NAME = process.env.BUCKET_NAME;
const BUCKET_KEY = process.env.BUCKET_KEY;
const CONCURRENCY_LIMIT = 10;

const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.ACCESS_KEY_ID,
    secretAccessKey: process.env.SECRET_ACCESS_KEY,
  },
});

function listFiles(directory) {
  return fs.readdirSync(directory).map((file) => path.join(directory, file));
}

async function uploadFile(filePath) {
  const fileStream = fs.createReadStream(filePath);
  const s3Key = path.posix.join(BUCKET_KEY, path.basename(filePath));

  const params = {
    Bucket: BUCKET_NAME,
    Key: s3Key,
    Body: fileStream,
  };

  try {
    await s3.send(new PutObjectCommand(params));
    //console.log(`✅ Upload: ${filePath} → s3://${BUCKET_NAME}/${s3Key}`);
  } catch (err) {
    console.error(`❌ Error uploading ${filePath}:`, err);
  }
}

async function uploadFilesWithLimit(directory) {
  const files = listFiles(directory);
  const limit = pLimit(CONCURRENCY_LIMIT);

  await Promise.all(files.map((file) => limit(() => uploadFile(file))));
}

(async () => {
  console.time("Total time");
  await uploadFilesWithLimit("directory");
  console.timeEnd("Total time");
})();
