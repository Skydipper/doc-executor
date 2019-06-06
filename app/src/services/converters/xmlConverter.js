const logger = require('logger');
const xml = require('xml-json');
const fs = require('fs');
const UrlNotFound = require('errors/urlNotFound');
const randomstring = require('randomstring');
const DownloadService = require('services/downloadService');
const FileNotFound = require('errors/fileNotFound');

class XMLConverter {

    constructor(url, dataPath, verify) {
        this.checkURL = new RegExp('^(https?:\\/\\/)?' + // protocol
            '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.?)+[a-z]{2,}|' + // domain name
            '((\\d{1,3}\\.){3}\\d{1,3}))' + // OR ip (v4) address
            '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' + // port and path
            '(\\?[:;&a-z\\d%_.~+=-]*)?' + // query string
            '(\\#[-a-z\\d_]*)?$', 'i');
        this.dataPath = dataPath;
        this.url = url;
        this.verify = verify;
    }

    async init() {
        if (this.checkURL.test(this.url)) {
            logger.debug('Is a url. Downloading file');
            const exists = await DownloadService.checkIfExists(this.url);
            if (!exists) {
                throw new UrlNotFound(400, 'Url not found');
            }
            const result = await DownloadService.downloadFile(this.url, `${randomstring.generate()}.xml`, this.verify);
            this.filePath = result.path;
            this.sha256 = result.sha256;
        } else {
            this.filePath = this.url;
        }
    }

    serialize() {
        if (!fs.existsSync(this.filePath)) {
            throw new FileNotFound(`File ${this.filePath} does not exist`);
        }
        const readStream = fs.createReadStream(this.filePath)
            .pipe(xml(this.dataPath));
        readStream.on('end', () => {
            if (fs.existsSync(this.filePath) && !this.verify) {
                logger.info('Removing file');
                fs.unlinkSync(this.filePath);
            }
        });

        return readStream;
    }

}

module.exports = XMLConverter;
