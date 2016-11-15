'use strict';

const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');
const json2csv = require('json2csv');
const fs = require('fs');

var Terraformer = require('terraformer-wkt-parser');
const csvSerializer = require('serializers/csvSerializer');
const OBTAIN_GEOJSON = /[.]*st_geomfromgeojson*\( *['|"]([^\)]*)['|"] *\)/g;
const CONTAIN_INTERSEC = /[.]*([and | or]*st_intersects.*)\)/g;

var unlink = function(file) {
    return function(callback) {
        fs.unlink(file, callback);
    };
};
function capitalizeFirstLetter(text) {
    switch(text){
        case 'multipolygon': 
            return 'MultiPolygon';
        case 'polygon':
            return 'Polygon';
        case 'point':
            return 'Point';
        case 'linestring':
            return 'LineString';
        case 'multipoint':
            return 'MultiPoint';
        case 'multilinestring':
            return 'MultiPointString';
        case 'geometrycollection':
            return 'GeometryCollection';
        default:
            return text;
    }

}

class QueryService {

    constructor() {
        logger.info('Connecting with elasticsearch');

        var sqlAPI = {
            sql: function(opts) {
                return function(cb) {
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI('/_sql'),
                        body: opts.sql
                    }, cb);
                }.bind(this);
            },
            explain: function(opts) {
                return function(cb) {
                    var call = function(err, data){                        
                        cb(err, data ? JSON.parse(data) : null);
                    };
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI('/_sql/_explain'),
                        body: opts.sql
                    }, call);
                }.bind(this);
            },
            mapping: function(opts) {
                return function(cb) {
                    this.transport.request({
                        method: 'GET',
                        path: `${opts.index}/_mapping`
                    }, cb);
                }.bind(this);
            },
            delete: function(opts){
                return function(cb) {
                    this.transport.request({
                        method: 'DELETE',
                        path: `${opts.index}`
                    }, cb);
                }.bind(this);
            },
            createScroll: function(opts){
                return function(cb) {                   
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI(`${opts.index}/_search?scroll=${opts.duration}`),
                        body: JSON.stringify( opts.query)
                    }, cb);
                }.bind(this);
            },
            getScroll: function(opts){
                logger.debug('GETSCROLL ', opts);
                return function(cb) {
                    this.transport.request({
                        method: 'GET',
                        path: encodeURI(`_search/scroll?scroll=${opts.scroll}&scroll_id=${opts.scroll_id}`),
                    }, cb);
                }.bind(this);
            }
        };
        elasticsearch.Client.apis.sql = sqlAPI;

        this.elasticClient = new elasticsearch.Client({
            host: config.get('elasticsearch.host') + ':' + config.get('elasticsearch.port'),
            log: 'info',
            apiVersion: 'sql'
        });

    }

    convert2GeoJSON(obj){
        
        let result = obj;
        if(obj.features){
            result = obj.features[0].geometry;
        } else if(obj.geometry){
            result = obj.geometry;
        }
        result.type = capitalizeFirstLetter(result.type);

        return result;
    }

    convertGeoJSON2WKT(sql){
        CONTAIN_INTERSEC.lastIndex = 0;
        OBTAIN_GEOJSON.lastIndex = 0;
        let sqlLower = sql.toLowerCase();
        if (CONTAIN_INTERSEC.test(sqlLower)) {
            logger.debug('Contain intersec');
            CONTAIN_INTERSEC.lastIndex = 0;
            let resultIntersec = CONTAIN_INTERSEC.exec(sqlLower)[0];
            if (resultIntersec) {
                resultIntersec = resultIntersec.trim();
            }
            let pos = sqlLower.indexOf(resultIntersec);
            
            let intersectResult = '';
            if(resultIntersec.startsWith('and')){
                intersectResult += ' AND ';
            } else if(resultIntersec.startsWith('or')) {
                intersectResult += ' OR ';
            }
            let geojson = OBTAIN_GEOJSON.exec(sqlLower);
            if (geojson && geojson.length > 1){
                geojson = this.convert2GeoJSON(JSON.parse(geojson[1]));
                let wkt = Terraformer.convert(geojson);
                intersectResult += ` GEO_INTERSECTS(the_geom, "${wkt}")`;
            }
            
            const result = `${sql.substring(0, pos)} ${intersectResult} ${sql.substring(pos + resultIntersec.length, sql.length)}`.trim();
            logger.debug('Result sql', result);
            return result;
        }
        return sql;

    }

    * doQuery(sql){
        logger.info('Doing query...', sql);
        sql = this.convertGeoJSON2WKT(sql);
        let result = yield this.elasticClient.sql({sql: sql});
        return result;
    }

    convertDataToDownload(data, type, first, more){
        
        if (type === 'csv') {
            let json =  json2csv({
                data: data.data,
                hasCSVColumnTitle: first
            });
           return json;
        } else if (type === 'json'){
            let dataString = JSON.stringify(data);
            dataString = dataString.substring(9, dataString.length - 2); // remove {"data": [ and ]}
            if (first) {
                dataString = '{"data":[' + dataString;
            }
            if (more) {
                dataString +=',';
            } else {
                dataString += ']}';
            }
            return dataString;
        }
    }

    * downloadQuery(sql, index, datasetId, stream, type='json') {
        logger.info('Download with query...', sql);
        sql = this.convertGeoJSON2WKT(sql);
        logger.debug('Doing explain');
        let resultQueryElastic = yield this.elasticClient.explain({sql: sql});
        
        let limit = -1;
        if (sql.indexOf('limit') >= 0){
            limit = resultQueryElastic.size;
        } 
        
        if (resultQueryElastic.size > 1000 || limit === -1){
            resultQueryElastic.size = 1000;
        }
        logger.debug('Creating params to scroll with query', resultQueryElastic);
        let params = {
            query: resultQueryElastic,
            duration: '1m',
            index: index
        };
        logger.debug('Generating file');
        
        try{            
            let size = resultQueryElastic.size;
            logger.debug('Creating scroll');
            let resultScroll = yield this.elasticClient.createScroll(params);
            let first = true;
            let total = 0;
            while (resultScroll[0].hits && resultScroll[0].hits &&  resultScroll[0].hits.hits.length > 0 && (total < limit || limit === -1)){
                logger.debug('Writting data');
                let more = false;
                const data = csvSerializer.serialize(resultScroll, sql, datasetId);
                first = true;
                total += resultScroll[0].hits.hits.length;
                if (total < limit || limit === -1) {
                    resultScroll = yield this.elasticClient.getScroll({
                        scroll: '1m',
                        scroll_id: resultScroll[0]._scroll_id,
                    });
                    if(resultScroll[0].hits && resultScroll[0].hits &&  resultScroll[0].hits.hits.length > 0) {
                        more = true;                    
                    }
                } else {
                    more = false;
                }
                stream.write(this.convertDataToDownload(data, type, first, more), {encoding: 'binary'});
                
            }
            
            stream.end();
            logger.info('Download correctly');
            
        } catch(err){
            logger.error('Error generating file', err);
            throw err;
        }
    }

    * getMapping(index){
        logger.info('Obtaining mapping...');

        let result = yield this.elasticClient.mapping({index: index});
        return result;
    }

    * deleteIndex(index){
        logger.info('Deleting index %s...', index);

        let result = yield this.elasticClient.delete({index: index});
        return result;
    }
}

module.exports = new QueryService();
