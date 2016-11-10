'use strict';

const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');
const fs = require('fs');
var sleep = require('co-sleep');
var co = require('co');
var csv = require('fast-csv');
var uuid = require('uuid');
var _ = require('lodash');
var Promise = require('bluebird');

const CONTAIN_SPACES = /\s/g;

function isJSONObject(value) {
    if (isNaN(value) && /^[\],:{}\s]*$/.test(value.replace(/\\["\\\/bfnrtu]/g, '@').replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']').replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {
        return true;
    } else {
        return false;
    }
}

function saveBulk(client, requests) {
    return new Promise(function(resolve, reject ){
        client.bulk(requests, function(err, res) {
            if (err) {
                reject(err);
                return;
            }
            resolve(res);
        });
    });    
}

function createIndex(client, options) {
    return new Promise(function(resolve, reject) {
        client.indices.create(options, function(error, response) {
            if (error) {
                reject(error);
                return;
            }
            resolve(response);
        });
    });
}


function activateRefreshIndex(client, index){
    return new Promise(function(resolve, reject) {
        let options = {
            index: index,
            body: {
                    index: {
                        refresh_interval: '1s'
                    }
                }
        };
        client.indices.putSettings(options, function(error, response) {
            if (error) {
                reject(error);
                return;
            }
            resolve(response);
        });
    });
}

function desactivateRefreshIndex(client, index){
    return new Promise(function(resolve, reject) {
        let options = {
            index: index,
            body: {
                    index: {
                        refresh_interval: '-1'
                    }
                }
        };
        client.indices.putSettings(options, function(error, response) {
            if (error) {
                reject(error);
                return;
            }
            resolve(response);
        });
    });
}

class CSVImporter {
    constructor(filePath, index, type, polygon, point) {
        this.filePath = filePath;
        this.polygon = polygon;
        this.point = point;
        this.options = {
            index: index,
            type: type,
            csv: {
                headers: true
            }
        };
        this.elasticClient = new elasticsearch.Client({
            host: config.get('elasticsearch.host') + ':' + config.get('elasticsearch.port'),
            log: 'info'
        });
    }

    * activateRefreshIndex(index) {
        yield activateRefreshIndex(this.client, index);
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
    initImport() {
        logger.info('Checking mapping');
        if (this.polygon || this.point) {
            logger.info('Contain a geojson column', this.polygon, this.point);
            let body = {
                mappings: {
                    [this.options.type]: {
                        properties: {
                            the_geom: {
                                type: 'geo_shape'
                            }
                        }
                    }
                }
            };

            yield createIndex(this.elasticClient, {
                index: this.options.index,
                body: body
            });
            yield desactivateRefreshIndex(this.elasticClient, this.options.index);
        }
    }

    saveData(requests) {
        return co(function*() {
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

        });
    }

    *
    start() {
        yield this.initImport();
        let i = 0;
        return new Promise(function(resolve, reject) {
            try {
                let request = {
                    body: []
                };
                let stream = fs.createReadStream(this.filePath)
                    .pipe(csv({
                        headers: true,
                        discardUnmappedColumns: true
                    }))
                    .on('data', function(data) {
                        stream.pause();
                        if (_.isPlainObject(data)) {

                            let index = {
                                index: {
                                    _index: this.options.index,
                                    _type: this.options.type,
                                    _id: uuid.v4()
                                }
                            };


                            try {
                                let error = false;
                                _.forEach(data, function(value, key) {
                                    let newKey = key;
                                    try {

                                        if (CONTAIN_SPACES.test(key)) {
                                            delete data[key];
                                            newKey = key.replace(CONTAIN_SPACES, '_');
                                        }
                                        if (isJSONObject(value)) {
                                            data[newKey] = JSON.parse(value);
                                        } else if (!isNaN(value)) {
                                            data[newKey] = Number(value);
                                        } else {
                                            data[newKey] = value;
                                        }
                                    } catch (e) {
                                        logger.error(e);
                                        error = true;
                                        throw new Error(e);
                                    }
                                });
                                // logger.info('Variable error es', error);
                                if (!error) {
                                    // logger.info('Esta metiendo el elemento');
                                    if (this.point) {
                                        data.the_geom = this.convertPointToGeoJSON(data[this.point.lat], data[this.point.long]);
                                    } else if (this.polygon) {
                                        data.the_geom = this.convertPolygonToGeoJSON(data[this.polygon]);
                                    }
                                    request.body.push(index);
                                    request.body.push(data);
                                }
                            } catch (e) {
                                //continue
                            }

                        } else {
                            stream.end();
                            reject(new Error('Data and/or options have no headers specified'));
                        }

                        if (request.body && request.body.length >= 80000) {
                            logger.debug('Saving');
                            this.saveData(request).then(function(){
                                request.body = [];
                                stream.resume();
                                i++;
                                logger.debug('Pack saved successfully, num:', i);
                            }, function(err){
                                logger.error('Error saving ', err);
                                stream.end();
                                reject(err);
                            });
                        } else {
                            stream.resume();
                        }


                    }.bind(this))
                    .on('end', function() {
                        if (request.body && request.body.length > 0) {
                            this.saveData(request).then(function(res){
                                resolve(res);
                                logger.debug('Pack saved successfully');
                            }, function(err){
                                logger.error('Error saving ', err);
                                reject(err);
                                return;
                            });
                        }
                    }.bind(this));
            } catch (e)  {
                logger.error(e);
                reject(e);
            }

        }.bind(this));
    }
}

module.exports = CSVImporter;
