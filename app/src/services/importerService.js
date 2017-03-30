'use strict';

const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');
const fs = require('fs');
const sleep = require('co-sleep');
const co = require('co');
const csv = require('fast-csv');
const uuid = require('uuid');
const _ = require('lodash');
const queryService = require('services/queryService');
const ConverterFactory = require('services/converters/converterFactory');

const CONTAIN_SPACES = /\s/g;

function isJSONObject(value) {
    if (isNaN(value) && /^[\],:{}\s]*$/.test(value.replace(/\\["\\\/bfnrtu]/g, '@').replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']').replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {
        return true;
    } else {
        return false;
    }
}

function saveBulk(client, requests) {
    return new Promise(function (resolve, reject) {
        client.bulk(requests, function (err, res) {
            if (err) {
                reject(err);
                return;
            }
            resolve(res);
        });
    });
}

function createIndex(client, options) {
    return new Promise(function (resolve, reject) {
        client.indices.create(options, function (error, response) {
            if (error) {
                reject(error);
                return;
            }
            resolve(response);
        });
    });
}


function activateRefreshIndex(client, index) {
    return new Promise(function (resolve, reject) {
        let options = {
            index: index,
            body: {
                index: {
                    refresh_interval: '1s',
                    number_of_replicas: 1
                }
            }
        };
        client.indices.putSettings(options, function (error, response) {
            if (error) {
                reject(error);
                return;
            }
            resolve(response);
        });
    });
}

function desactivateRefreshIndex(client, index) {
    return new Promise(function (resolve, reject) {
        let options = {
            index: index,
            body: {
                index: {
                    refresh_interval: '-1',
                    number_of_replicas: 0
                }
            }
        };
        client.indices.putSettings(options, function (error, response) {
            if (error) {
                reject(error);
                return;
            }
            resolve(response);
        });
    });
}

class ImporterService {
    constructor(type, url, index, legend, dataPath, action) {
        logger.debug(`Creating importerservice with ${url} and dataPath ${dataPath} and action ${action}`);
        this.action = action;
        this.dataPath = dataPath;
        this.converter = ConverterFactory.getInstance(type, url, dataPath);
        this.legend = legend;
        this.index = index;
        this.options = {
            index: index,
            type: index
        };
        this.elasticClient = new elasticsearch.Client({
            host: config.get('elasticsearch.host') + ':' + config.get('elasticsearch.port'),
            log: 'error'
        });
    }

    * startProcess() {
        try {
            if (this.action === 'overwrite') {
                logger.info('Overwrite data. Remove old');
                yield queryService.deleteIndex(this.index);
                logger.info('Deleted successfully. Continue importing');
            }

            yield this.start(this.action !== 'concat');

            logger.info('Activating refresh index', this.index);
            yield this.activateRefreshIndex(this.index);
        } catch (err) {
            if (this.action !== 'concat') {
                logger.info('Throwed ERROR: Removing index ');
                yield queryService.deleteIndex(this.index);
            }
            throw err;
        }
    }

    * activateRefreshIndex(index) {
        yield activateRefreshIndex(this.elasticClient, index);
    }
    convertPointToGeoJSON(lat, long) {
        return {
            type: 'point',
            coordinates: [
                long,
                lat
            ]
        };
    }

    convertPolygonToGeoJSON(polygon) {
        let theGeom = null;
        if (polygon.features) {
            theGeom = polygon.features[0].geometry;
        } else if (polygon.geometry) {
            theGeom = polygon.geometry;
        }
        theGeom.type = theGeom.type.toLowerCase();
        return theGeom;
    }

    *
    initImport(generateIndex) {

        if (generateIndex) {
            logger.info('Checking mapping');
            let body = {
                mappings: {
                    [this.options.type]: {
                        properties: {

                        }
                    }
                }
            };
            // if (this.legend && this.legend.date && this.legend.date.length > 0) {
            //     for (let i = 0, length = this.legend.date.length; i < length; i++) {
            //         body.mappings[this.options.type].properties[this.legend.date[i]] = {
            //             type: 'date'
            //         };
            //     }
            // }

            if (this.legend && this.legend.lat &&  this.legend.long) {
                logger.info('Contain a geojson column', this.legend.lat);
                body.mappings[this.options.type].properties.the_geom = {
                    type: 'geo_shape'
                };
                logger.info('Generating the_geom_point column');
                body.mappings[this.options.type].properties.the_geom_point = {
                    type: 'geo_point'
                };

            }

            logger.info('Creating index ', this.options.index);
            yield createIndex(this.elasticClient, {
                index: this.options.index,
                body: body
            });
        }

        logger.debug('Deactivating refresh index');
        yield desactivateRefreshIndex(this.elasticClient, this.options.index);
        logger.debug('Init converter');
        yield this.converter.init();
    }

    saveData(requests) {
        return co(function* () {
            for (let i = 1; i < 4; i++) {
                try  {
                    yield saveBulk(this.elasticClient, requests);
                    return;
                } catch (e) {
                    if (e.status !== 408 || i === 3) {
                        throw e;
                    } else {
                        logger.info('Waiting ', 30000 * i);
                        yield sleep(30000 * i);
                    }
                }
            }

        }.bind(this));
    }

    * start(generateIndex) {
        logger.debug('Start with generateIndex: ', generateIndex);
        yield this.initImport(generateIndex);
        let i = 0;
        return new Promise(function (resolve, reject) {
            try {
                logger.debug('Starting read file');
                let request = {
                    body: []
                };
                let stream = this.converter.serialize()
                    .on('data', function (data) {
                        stream.pause();
                        if (_.isPlainObject(data)) {

                            let index = {
                                index: {
                                    _index: this.options.index,
                                    _type: this.options.type
                                }
                            };

                            try {
                                let error = false;
                                _.forEach(data, function (value, key) {
                                    let newKey = key;
                                    try {
                                        if (newKey !== '_id') {
                                            if (CONTAIN_SPACES.test(key)) {
                                                delete data[key];
                                                newKey = key.replace(CONTAIN_SPACES, '_');
                                            }
                                            if (!(value instanceof Object) && isJSONObject(value)) {
                                                try {
                                                    data[newKey] = JSON.parse(value);
                                                } catch (e) {
                                                    data[newKey] = value;
                                                }
                                            } else if (!isNaN(value)) {
                                                data[newKey] = Number(value);
                                            } else {
                                                data[newKey] = value;
                                            }
                                        } else {
                                            delete data[newKey];
                                        }

                                    } catch (e) {
                                        logger.error(e);
                                        error = true;
                                        throw new Error(e);
                                    }
                                });
                                if (!error) {
                                    if (this.legend && (this.legend.lat || this.legend.long)) {
                                        data.the_geom = this.convertPointToGeoJSON(data[this.legend.lat], data[this.legend.long]);
                                        data.the_geom_point = {
                                            lat: data[this.legend.lat],
                                            lon: data[this.legend.long]
                                        };
                                    }
                                    request.body.push(index);
                                    request.body.push(data);
                                }
                            } catch (e) {
                                //continue
                                logger.error('Error generating', e);
                            }

                        } else {
                            stream.end();
                            reject(new Error('Data and/or options have no headers specified'));
                        }

                        if (request.body && request.body.length >= 80000) {
                            logger.debug('Saving');
                            this.saveData(request).then(function () {
                                request.body = [];
                                stream.resume();
                                i++;
                                logger.debug('Pack saved successfully, num:', i);
                            }, function (err) {
                                logger.error('Error saving ', err);
                                stream.end();
                                reject(err);
                            });
                        } else {
                            stream.resume();
                        }


                    }.bind(this))
                    .on('end', function () {
                        if (request.body && request.body.length > 0) {
                            this.saveData(request).then(function (res) {
                                resolve(res);
                                logger.debug('Pack saved successfully');
                            }, function (err) {
                                logger.error('Error saving ', err);
                                reject(err);
                                return;
                            });
                        } else {
                            logger.debug('Pack saved successfully');
                            resolve();
                        }
                    }.bind(this));
            } catch (e)  {
                logger.error(e);
                reject(e);
            }

        }.bind(this));
    }
}

module.exports = ImporterService;
