const logger = require('logger');
var xml = require('xml-json');
const fs = require('fs');
const UrlNotFound = require('errors/urlNotFound');
const randomstring = require('randomstring');
const DownloadService = require('services/downloadService');


class XMLConverter {
    constructor(url, dataPath) {
        this.checkURL = new RegExp('^(?:[a-z]+:)?//', 'i');
        this.dataPath = dataPath;
        this.url = url;
    }

    * init() {
        if (this.checkURL.test(this.url))  {
            logger.debug('Is a url. Downloading file');
            const exists = yield DownloadService.checkIfExists(this.url);
            if (!exists) {
                throw new UrlNotFound(400, 'Url not found');
            }
            this.filePath = yield DownloadService.downloadFile(this.url, randomstring.generate() + '.xml');
        } else {
            this.filePath = this.url;
        }
    }

    serialize() {
        const readStream = fs.createReadStream(this.filePath)
            .pipe(xml(this.dataPath));
        readStream.on('end', () => {
            if (fs.existsSync(this.filePath)) {
                logger.info('Removing file');
                fs.unlinkSync(this.filePath);
            }
        });

        return readStream;
    }
}

module.exports = XMLConverter;
