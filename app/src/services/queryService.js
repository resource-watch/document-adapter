'use strict';

const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');
const json2csv = require('json2csv');
const fs = require('fs');

var Terraformer = require('terraformer-wkt-parser');
const csvSerializer = require('serializers/csvSerializer');
const DeleteSerializer = require('serializers/deleteSerializer');
const OBTAIN_GEOJSON = /[.]*st_geomfromgeojson*\( *['|"]([^\)]*)['|"] *\)/g;
const CONTAIN_INTERSEC = /[.]*([and | or]*st_intersects.*)\)/g;

const IndexNotFound = require('errors/IndexNotFound');

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

class Scroll {
    constructor(elasticClient, sql, index, datasetId, stream, download, cloneUrl, type){
        this.elasticClient = elasticClient;
        this.sql = sql;
        this.index = index;
        this.datasetId = datasetId;
        this.stream = stream;
        this.download = download;
        this.cloneUrl = cloneUrl;
        this.type = type || 'json';
        this.timeout = false;
    }

    * init(){
        this.timeoutFunc = setTimeout(function(){
            this.timeout = true;
        }.bind(this), 60000);
        let resultQueryElastic = yield this.elasticClient.explain({sql: this.sql});
        
        this.limit = -1;
        if (this.sql.toLowerCase().indexOf('limit') >= 0){
            this.limit = resultQueryElastic.size;
        } 
        
        if (resultQueryElastic.size > 10000 || this.limit === -1){
            resultQueryElastic.size = 10000;
        }
        logger.debug('Creating params to scroll with query', resultQueryElastic);
        let params = {
            query: resultQueryElastic,
            duration: '1m',
            index: this.index
        };
        
        try{            
            let size = resultQueryElastic.size;
            logger.debug('Creating scroll');
            this.resultScroll = yield this.elasticClient.createScroll(params);
            this.first = true;
            this.total = 0;        
            
        } catch(err){
            if (err.statusCode === 404) {
                throw new IndexNotFound(404, 'Table not found');
            }
            throw err;
        }
    }

    convertDataToDownload(data, type, first, more, cloneUrl){
        
        if (type === 'csv') {
            let json = json2csv({
                data: data.data,
                hasCSVColumnTitle: first
            }) + '\n';
           return json;
        } else if (type === 'json'){
            let dataString = '';
            if (data) {
                dataString = JSON.stringify(data);
                dataString = dataString.substring(9, dataString.length - 2); // remove {"data": [ and ]}
            }
            if (first) {
                dataString = '{"data":[' + dataString;
            }
            if (more) {
                dataString +=',';
            } else {
                
                if(!this.download) {
                    dataString += '],';
                    var meta= {
                        cloneUrl: cloneUrl
                    };
                    
                    dataString += `"meta": ${JSON.stringify(meta)} }`;
                } else { 
                    dataString += ']}';
                }                
            }
            return dataString;
        }
    }
    * continue(){
        
        if (this.resultScroll[0].aggregations) {
            const data = csvSerializer.serialize(this.resultScroll, this.sql, this.datasetId);
            this.stream.write(this.convertDataToDownload(data, this.type, true, false, this.cloneUrl, {encoding: 'binary'}));
        } else {
           
            while (!this.timeout && this.resultScroll[0].hits && this.resultScroll[0].hits &&  this.resultScroll[0].hits.hits.length > 0 && (this.total < this.limit || this.limit === -1)){
                    logger.debug('Writting data');
                    let more = false;
                    const data = csvSerializer.serialize(this.resultScroll, this.sql, this.datasetId);
                    
                    this.first = true;
                    this.total += this.resultScroll[0].hits.hits.length;
                    if (this.total < this.limit || this.limit === -1) {
                        this.resultScroll = yield this.elasticClient.getScroll({
                            scroll: '1m',
                            scroll_id: this.resultScroll[0]._scroll_id,
                        });
                        if(this.resultScroll[0].hits && this.resultScroll[0].hits &&  this.resultScroll[0].hits.hits.length > 0) {
                            more = true;                    
                        }
                    } else {
                        more = false;
                    }
                    this.stream.write(this.convertDataToDownload(data, this.type, this.first, more, this.cloneUrl, {encoding: 'binary'}));
                    
            }
            if (this.total === 0) {
                this.stream.write(this.convertDataToDownload(null, this.type, true, false, this.cloneUrl, {encoding: 'binary'}));
            }
        }
        this.stream.end();
        if(this.timeout){
            throw new Error('Timeout exceed');
        }
        clearTimeout(this.timeoutFunc);
        
        logger.info('Write correctly');
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
                        if (data) {
                            try{
                                data = JSON.parse(data);
                            } catch(e){
                                data = null;
                            }
                        }                    
                        cb(err, data);
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
            },
            deleteByQuery: function(opts){
                logger.debug('Delete by query ', opts);
                return function(cb) {
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI(`${opts.index}/_delete_by_query`),
                        body: JSON.stringify( opts.body)
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

    * doQuery(sql, index, datasetId, body, cloneUrl){        
        logger.info('Doing query...');
        sql = this.convertGeoJSON2WKT(sql);
        var scroll = new Scroll(this.elasticClient, sql, index, datasetId, body, false, cloneUrl);
        yield scroll.init();
        yield scroll.continue();
        logger.info('Finished query');    
    }    

    * doDeleteQuery(where, tableName) {
        logger.info(`Doing delete to ${tableName} with where `, where);
        logger.debug('Obtaining explain with select ', `select * from ${tableName} where ${where.expression}`);
        try {
            let resultQueryElastic = yield this.elasticClient.explain({sql: `select * from ${tableName} where ${where.expression}`});
            delete resultQueryElastic.from;
            delete resultQueryElastic.size;
            logger.debug('Doing query');
            let result = yield this.elasticClient.deleteByQuery({index: tableName, body: resultQueryElastic});
            logger.debug(result[0]);
            return DeleteSerializer.serialize(result[0]);
        } catch(e){
            logger.error(e);
            throw new Error('Query not valid');
        }
        
    }

    * downloadQuery(sql, index, datasetId, body, type='json') {
        logger.info('Download with query...');
        sql = this.convertGeoJSON2WKT(sql);
        var scroll = new Scroll(this.elasticClient, sql, index, datasetId, body, false, null, type);
        yield scroll.init();
        yield scroll.continue();
        logger.info('Finished query');
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
