'use strict';

const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');
const fs = require('fs');
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

function createIndex(client, options){
    return new Promise(function(resolve, reject){
        client.indices.create(options, function(error, response){
            if(error){
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

    convertPointToGeoJSON(lat, long){
        return {
            type: 'point',
            coordinates: [
              long,
              lat
            ]
        };
    }

    convertPolygonToGeoJSON(polygon){
        let theGeom = null;
        if(polygon.features){
            theGeom = polygon.features[0].geometry;
        } else if(polygon.geometry){
            theGeom = polygon.geometry;
        }
        theGeom.type = theGeom.type.toLowerCase();
        return theGeom;
    }

    * initImport(){
        logger.info('Checking mapping');
        if(this.polygon || this.point){
            logger.info('Contain a geojson column', this.polygon, this.point);
            let body = {
                mappings:{
                    [this.options.type]:{
                        properties:{
                            the_geom: {
                                type: 'geo_shape'
                            }
                        }
                    }
                }
            };

            yield createIndex(this.elasticClient, { index: this.options.index, body: body});
        }
    }

    * start() {
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
                                    try{
                                        
                                        if(CONTAIN_SPACES.test(key)){
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
                                    }catch(e){
                                        logger.error(e);
                                        error = true;
                                        throw new Error(e);                                        
                                    }
                                });
                                logger.info('Variable error es', error);
                                if (!error){
                                    logger.info('Esta metiendo el elemento');
                                    if(this.point) {
                                        data.the_geom = this.convertPointToGeoJSON(data[this.point.lat], data[this.point.long]);
                                    } else if (this.polygon){
                                        data.the_geom = this.convertPolygonToGeoJSON(data[this.polygon]);
                                    }
                                    request.body.push(index);
                                    request.body.push(data);
                                }
                            } catch(e){
                                //continue
                            }

                        } else {
                            stream.end();
                            reject(new Error('Data and/or options have no headers specified'));
                        }

                        if (request.body && request.body.length >= 25000) {
                            logger.debug('Saving');
                            this.elasticClient.bulk(request, function(err, res) {
                                if (err) {
                                    logger.error('Error saving ', err);
                                    stream.end();
                                    reject(err);
                                    return;
                                }
                                request.body = [];
                                stream.resume();
                                i++;
                                logger.debug('Pack saved successfully, num:', i);
                            });
                        }else {
                            stream.resume();
                        }


                    }.bind(this))
                    .on('end', function() {
                        if(request.body && request.body.length > 0){
                            this.elasticClient.bulk(request, function(err, res) {
                                if (err) {
                                    reject(err);
                                    return;
                                }
                                resolve(res);
                                logger.debug('Pack saved successfully');
                            });
                        }
                    }.bind(this));
            } catch(e) {
                logger.error(e);
                reject(e);
            }

        }.bind(this));
    }
}

module.exports = CSVImporter;
