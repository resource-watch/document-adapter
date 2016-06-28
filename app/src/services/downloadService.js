'use strict';

const coRequest = require('co-request');
const request = require('request');
const fs = require('fs');
const logger = require('logger');
const randomstring = require('randomstring');

let requestDownloadFile = function(url, path) {
    return function(cb) {
        var r = request.get(url, function(error, response, body) {
            cb(error, {response: response, body: body});
        });
        // no guardar el fichero
        r.pipe(fs.createWriteStream(path));
    };
};

class DownloadService {

    static * checkIfCSV(url){
        logger.info('Checking if the content-type is text/csv');
        let result = yield coRequest.head(url);
        logger.info(result.headers['content-type']);
        logger.debug('POsition', result.headers['content-type'].indexOf('text/csv'));
        if (result.headers['content-type'] && result.headers['content-type'].indexOf('text/csv') === -1) {
            return false;
        }
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
        return path;
    }


}

module.exports = DownloadService;
