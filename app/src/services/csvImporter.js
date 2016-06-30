'use strict';

const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');
const fs = require('fs');
var csv = require('fast-csv');
var uuid = require('uuid');
var _ = require('lodash');
var Promise = require('bluebird');

function isJSONObject(value) {
    if (isNaN(value) && /^[\],:{}\s]*$/.test(value.replace(/\\["\\\/bfnrtu]/g, '@').replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']').replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {
        return true;
    } else {
        return false;
    }
}

class CSVImporter {
    constructor(filePath, index, type) {
        this.filePath = filePath;
        this.options = {
            index: index,
            type: type,
            csv: {
                headers: true
            }
        };
    }

    start() {
        var elasticClient = new elasticsearch.Client({
            host: config.get('elasticsearch.host') + ':' + config.get('elasticsearch.port'),
            log: 'info'
        });
        return new Promise(function(resolve, reject) {
            let request = {
                body: []
            };
            let stream = fs.createReadStream(this.filePath)
                .pipe(csv({
                    headers: true
                }))
                .on('data', function(data) {
                    stream.pause();
                    if (_.isPlainObject(data)) {

                        request.body.push({
                            index: {
                                _index: this.options.index,
                                _type: this.options.type,
                                _id: uuid.v4()
                            }
                        });
                        _.forEach(data, function(value, key) {
                            if (isJSONObject(value)) {
                                data[key] = JSON.parse(value);
                            } else if (!isNaN(value)) {
                                data[key] = Number(value);
                            }
                        });
                        request.body.push(data);

                    } else {
                        stream.end();
                        reject(new Error('Data and/or options have no headers specified'));
                    }

                    if (request.body && request.body.length >= 25000) {
                        logger.debug('Saving');
                        elasticClient.bulk(request, function(err, res) {
                            if (err) {
                                logger.error('Error saving ', err);
                                stream.end();
                                reject(err);
                                return;
                            }
                            request.body = [];
                            stream.resume();
                            logger.debug('Pack saved successfully');
                        });
                    }else {
                        stream.resume();
                    }


                }.bind(this))
                .on('end', function() {
                    if(request.body && request.body.length > 0){
                        elasticClient.bulk(request, function(err, res) {
                            if (err) {
                                reject(err);
                                return;
                            }
                            resolve(res);
                            logger.debug('Pack saved successfully');
                        });
                    }
                }.bind(this));

        }.bind(this));
    }
}

module.exports = CSVImporter;
