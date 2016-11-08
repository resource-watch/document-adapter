'use strict';

const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');
var Terraformer = require('terraformer-wkt-parser');

const OBTAIN_GEOJSON = /[.]*st_geomfromgeojson*\( *['|"]([^\)]*)['|"] *\)/g;
const CONTAIN_INTERSEC = /[.]*([and | or]*st_intersects.*)\)/g;

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
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI('/_sql/_explain'),
                        body: opts.sql
                    }, cb);
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
