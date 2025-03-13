import fs from "fs";
import path from "path";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
import { Agent } from "https";

dotenv.config();

const BUCKET_NAME = process.env.BUCKET_NAME;
const BUCKET_KEY = process.env.BUCKET_KEY;

const s3 = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.ACCESS_KEY_ID,
        secretAccessKey: process.env.SECRET_ACCESS_KEY,
    },
    requestHandler: new Agent({
        maxSockets: 200,
        keepAlive: true,
    }),
});

function listFiles(directory) {
    return fs.readdirSync(directory).map((file) => path.join(directory, file));
}

async function uploadFile(filePath) {
    if (!filePath) {
        console.error("❌ Error: filePath es undefined");
        return;
    }
    const fileStream = fs.createReadStream(filePath);
    const s3Key = path.posix.join(BUCKET_KEY, path.basename(filePath));

    const params = {
        Bucket: BUCKET_NAME,
        Key: s3Key,
        Body: fileStream,
    };

    try {
        await s3.send(new PutObjectCommand(params));
        //console.log(`✅ UPLOAD: ${filePath} → s3://${BUCKET_NAME}/${s3Key}`);
    } catch (err) {
        console.error(`❌ Error Uploading ${filePath}:`, err);
    }
}

async function uploadFilesParallel(directory) {
    const files = listFiles(directory);
    await Promise.all(files.map(uploadFile));
}

(async () => {
    console.time("Total time");
    await uploadFilesParallel("directory");
    console.timeEnd("Total time");
})();
