'use strict';

const coRequest = require('co-request');
const request = require('request');
const fs = require('fs');
const logger = require('logger');
const randomstring = require('randomstring');
const Bluebird = require('bluebird');
const http = require('http');

function humanFileSize(bytes, si) {
    var thresh = si ? 1000 : 1024;
    if(Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }
    var units = si ? ['kB','MB','GB','TB','PB','EB','ZB','YB'] : ['KiB','MiB','GiB','TiB','PiB','EiB','ZiB','YiB'];
    var u = -1;
    do {
        bytes /= thresh;
        ++u;
    } while(Math.abs(bytes) >= thresh && u < units.length - 1);
    return bytes.toFixed(1)+' '+units[u];
}

let requestDownloadFile = function(url, path) {
    return new Bluebird(function(resolve, reject){
        logger.debug('Sending request');
        try {
            let oldSize = 0;
            var requestserver = http.request(url, function(r) {
                logger.debug('STATUS: ' + r.statusCode);
                logger.debug('HEADERS: ' + JSON.stringify(r.headers));
                logger.debug('Initializing downlaod data');
                var fd = fs.openSync(path, 'w');
                let size = 0;
                r.on('data', function (chunk) {

                    size += chunk.length;
                    if(size - oldSize > (1024 * 1024 *100)) {
                        oldSize = size;
                        logger.debug(humanFileSize(size)+' received');
                    }

                    fs.write(fd, chunk, 0, chunk.length, null, function(er, written) {
                    });
                });
                r.on('error',function(e){
                    logger.error('Error downloading file', e);
                    reject(e);
                });
                r.on('end',function(){
                    logger.info(humanFileSize(size)+' downloaded. Ended from server');
                    fs.closeSync(fd);

                    resolve();
                });
            });
            requestserver.end();
        } catch(err){
            logger.error(err);
            reject(err);
        }
    });

};

class DownloadService {

    static * checkIfCSV(url){
        logger.info('Checking if the content-type is text/csv');
        let result = yield coRequest.head(url);
        logger.debug('Headers ', result.headers['content-type']);
        // if (result.headers['content-type'] && result.headers['content-type'].indexOf('text/csv') === -1 && result.headers['content-type'].indexOf('text/plain') === -1 && result.headers['content-type'].indexOf('text/tab-separated-values') === -1) {
        //     return false;
        // }
        return true;
    }

    static * downloadFile(url) {
        if(! (yield DownloadService.checkIfCSV(url))){
            throw new Error('File is not text/csv');
        }
        logger.debug('Type text/csv. Downloading....');
        let path = '/tmp/' + randomstring.generate() + '.csv';
        logger.debug('Temporal path', path, '. Downloading');
        let result = yield requestDownloadFile(url, path);
        logger.debug('Download file!!!');
        return path;
    }


}

module.exports = DownloadService;
