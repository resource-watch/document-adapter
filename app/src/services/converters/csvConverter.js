const logger = require('logger');
const csv = require('fast-csv');
const fs = require('fs');
const UrlNotFound = require('errors/urlNotFound');
const randomstring = require('randomstring');
const DownloadService = require('services/downloadService');


class CSVConverter {
    constructor(url, delimiter = ',') {
        this.checkURL = new RegExp('^(?:[a-z]+:)?//', 'i');
        this.delimiter = delimiter;
        this.url = url;
    }

    * init() {
        if (this.checkURL.test(this.url)) {
            logger.debug('Is a url. Downloading file');
            const exists = yield DownloadService.checkIfExists(this.url);
            if(!exists) {
                throw new UrlNotFound(400, 'Url not found');
            }
            let name = randomstring.generate();
            if (this.delimiter === '\t'){
                name += '.tsv';
            } else {
                name += '.csv';
            }
            this.filePath = yield DownloadService.downloadFile(this.url, name);
        } else {
            this.filePath = this.url;
        }
    }

    serialize() {        
        const readStream = csv.fromPath(this.filePath, {
                headers: true,
                delimiter: this.delimiter,
                discardUnmappedColumns: true
            });
        readStream.on('end', () => {
            logger.info('Removing file', this.filePath);
            if (fs.existsSync(this.filePath)){
                fs.unlinkSync(this.filePath);
            }
        });

        return readStream;
    }
}

module.exports = CSVConverter;
