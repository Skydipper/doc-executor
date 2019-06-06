const logger = require('logger');
const fs = require('fs');
const s3 = require('s3');

const S3_ACCESS_KEY_ID = process.env.S3_ACCESS_KEY_ID;
const S3_SECRET_ACCESS_KEY = process.env.S3_SECRET_ACCESS_KEY;

const S3Client = s3.createClient({
    maxAsyncS3: 20,     // this is the default
    s3RetryCount: 3,    // this is the default
    s3RetryDelay: 1000, // this is the default
    multipartUploadThreshold: 20971520, // this is the default (20 MB)
    multipartUploadSize: 15728640, // this is the default (15 MB)
    s3Options: {
        accessKeyId: S3_ACCESS_KEY_ID,
        secretAccessKey: S3_SECRET_ACCESS_KEY,
        region: 'us-east-1'
    }
});

class S3Service {

    static async upload(id, type, path) {

        try {
            logger.info('[SERVICE] Uploading to S3');
            const params = {
                localFile: path,
                s3Params: {
                    Bucket: 'wri-api-backups',
                    Key: `datasets/${id}.${type}`,
                    ACL: 'public-read'
                }
            };
            const uploader = S3Client.uploadFile(params);
            await new Promise((resolve) => {
                uploader.on('end', data => resolve(data));
            });
            return s3.getPublicUrlHttp(params.s3Params.Bucket, params.s3Params.Key);
        } catch (err) {
            throw err;
        } finally {
            fs.unlinkSync(path);
        }
    }

}

module.exports = S3Service;
