const logger = require('logger');
const JSONStream = require('JSONStream');
const fs = require('fs');
const randomstring = require('randomstring');
const DownloadService = require('services/downloadService');


class JSONConverter {
    constructor(url, dataPath) {
        logger.debug(`Creating jsonConverter with url ${url} and dataPath ${dataPath}`);
        this.dataPath = dataPath ? dataPath + '.*' : '*' ;
        this.checkURL = new RegExp('^(?:[a-z]+:)?//', 'i');
        this.url = url;
    }

    * init() {
        if (this.checkURL.test(this.url)) {
            logger.debug('Is a url. Downloading file in url ', this.url);
            this.filePath = yield DownloadService.downloadFile(this.url, randomstring.generate() + '.json');
            logger.debug('Temporal path ', this.filePath);
        } else {
            this.filePath = this.url;
        }
    }

    serialize() {        
        const readStream = fs.createReadStream(this.filePath)
            .pipe(JSONStream.parse(this.dataPath));
        readStream.on('end', () => {
            if (fs.existsSync(this.filePath)){
                logger.info('Removing file');
                fs.unlinkSync(this.filePath);
            }
        });

        return readStream;
    }
}

module.exports = JSONConverter;
