const requestPromise = require('request-promise');
const fs = require('fs');
const logger = require('logger');
const Bluebird = require('bluebird');
const https = require('https');
const http = require('http');
const crypto = require('crypto');

const algorithm = 'sha256';

function humanFileSize(bytes, si) {
    const thresh = si ? 1000 : 1024;
    if (Math.abs(bytes) < thresh) {
        return `${bytes} B`;
    }
    const units = si ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    do {
        bytes /= thresh;
        ++u;
    } while (Math.abs(bytes) >= thresh && u < units.length - 1);
    return `${bytes.toFixed(1)} ${units[u]}`;
}

const requestDownloadFile = function (url, path, verify) {

    return new Bluebird((resolve, reject) => {
        logger.debug('Sending request');
        try {
            let dlprogress = 0;
            let oldProgress = 0;
            let requestserver = null;
            if (url.trim().startsWith('https')) {
                requestserver = https.request(url);
            } else {
                requestserver = http.request(url);
            }
            requestserver.addListener('response', (response) => {
                const downloadfile = fs.createWriteStream(path, {
                    flags: 'a'
                });
                logger.info(`File size: ${humanFileSize(parseInt(response.headers['content-length'], 10))}`);
                let shasum = null;
                if (verify) {
                    shasum = crypto.createHash(algorithm);
                }
                response.addListener('data', chunk => {
                    if (verify) {
                        shasum.update(chunk);
                    }
                    dlprogress += chunk.length;
                    downloadfile.write(chunk, {
                        encoding: 'binary'
                    });
                    if (dlprogress - oldProgress > 100 * 1024 * 1024) {
                        logger.debug(`${humanFileSize(dlprogress)} progress`);
                        oldProgress = dlprogress;
                    }
                });
                response.addListener('end', () => {
                    downloadfile.end();
                    logger.info(`${humanFileSize(dlprogress)} downloaded. Ended from server`);
                    if (verify) {
                        const sha256 = shasum.digest('hex');
                        resolve(sha256);
                    } else {
                        resolve();
                    }

                });
                response.on('error', e => {
                    logger.error('Error downloading file', e);
                    reject(e);
                });

            });
            requestserver.end();
        } catch (err) {
            logger.error(err);
            reject(err);
        }
    });

};

class DownloadService {

    static async checkIfExists(url) {
        logger.info(`Checking if the url ${url} exists`);
        try {
            const result = await requestPromise.head({
                url,
                simple: false,
                resolveWithFullResponse: true
            });
            logger.debug('Headers ', result.headers['content-type'], result.statusCode);

            return result.statusCode === 200;
        } catch(err) {
            logger.error(err);
            return false;
        }
    }

    static async downloadFile(url, name, verify) {
        logger.debug('Downloading....');
        const path = `/tmp/${name}`;
        logger.debug('Temporal path', path, '. Downloading');
        const sha256 = await requestDownloadFile(url, path, verify);
        logger.debug('Downloaded file!!!');
        return {
            path,
            sha256
        };
    }


}

module.exports = DownloadService;
