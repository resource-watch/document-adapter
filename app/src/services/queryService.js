
const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');
const json2csv = require('json2csv');
const co = require('co');
const Json2sql = require('sql2json').json2sql;
const Terraformer = require('terraformer-wkt-parser');
const csvSerializer = require('serializers/csvSerializer');
const DocumentNotFound = require('errors/documentNotFound');
const IndexNotFound = require('errors/indexNotFound');
const ctRegisterMicroservice = require('ct-register-microservice-node');

const elasticUri = process.env.ELASTIC_URI || config.get('elasticsearch.host') + ':' + config.get('elasticsearch.port');

// function capitalizeFirstLetter(text) {
//     switch (text) {
//         case 'multipolygon':
//             return 'MultiPolygon';
//         case 'polygon':
//             return 'Polygon';
//         case 'point':
//             return 'Point';
//         case 'linestring':
//             return 'LineString';
//         case 'multipoint':
//             return 'MultiPoint';
//         case 'multilinestring':
//             return 'MultiPointString';
//         case 'geometrycollection':
//             return 'GeometryCollection';
//         default:
//             return text;
//     }
//
// }

class Scroll {

    constructor(elasticClient, sql, parsed, index, datasetId, stream, download, cloneUrl, format) {
        this.elasticClient = elasticClient;
        this.sql = sql;
        this.parsed = parsed;
        this.index = index;
        this.datasetId = datasetId;
        this.stream = stream;
        this.download = download;
        this.cloneUrl = cloneUrl;
        this.format = format || 'json';
        this.timeout = false;
    }

    * init() {
        this.timeoutFunc = setTimeout(function () {
            this.timeout = true;
        }.bind(this), 60000);
        const resultQueryElastic = yield this.elasticClient.explain({
            sql: this.sql
        });
        if (this.parsed.group) {
            logger.debug('Config size of aggregations');
            let name = null;
            let aggregations = resultQueryElastic.aggregations;
            let finish = false;
            while (aggregations){
                const keys = Object.keys(aggregations);
                if (keys.length === 1) {
                    if (aggregations[keys[0]] && aggregations[keys[0]].terms){
                        aggregations[keys[0]].terms.size = this.parsed.limit || 999999;
                        aggregations = aggregations[keys[0]].aggregations;
                    } else if (keys[0].indexOf('NESTED') >= -1) {
                        aggregations = aggregations[keys[0]].aggregations;
                    } else {
                        aggregations = null;
                    }
                } else {
                    aggregations = null;
                }
            }
        }
        this.limit = -1;
        if (this.sql.toLowerCase().indexOf('limit') >= 0) {
            this.limit = resultQueryElastic.size;
        }

        if (resultQueryElastic.size > 10000 || this.limit === -1) {
            resultQueryElastic.size = 10000;
        }
        logger.debug('Creating params to scroll with query', resultQueryElastic);
        const params = {
            query: resultQueryElastic,
            duration: '1m',
            index: this.index
        };

        try {
            const size = resultQueryElastic.size;
            logger.debug('Creating scroll');
            this.resultScroll = yield this.elasticClient.createScroll(params);
            this.first = true;
            this.total = 0;

        } catch (err) {
            if (err.statusCode === 404) {
                throw new IndexNotFound(404, 'Table not found');
            }
            throw err;
        }
    }

    convertDataToDownload(data, type, first, more, cloneUrl) {

            if (type === 'csv') {
                let json = json2csv({
                    data: data ? data.data : [],
                    hasCSVColumnTitle: first
                }) + '\n';
                return json;
            } else if (type === 'json' || type === 'geojson') {
                let dataString = '';
                if (data) {
                    dataString = JSON.stringify(data);
                    dataString = dataString.substring(9, dataString.length - 2); // remove {"data": [ and ]}
                }
                if (first) {
                    if (type === 'geojson') {
                        dataString = `{"data":[{"type": "FeatureCollection", "features": [${dataString}`;
                    } else {
                        dataString = '{"data":[' + dataString;
                    }
                }
                if (more) {
                    dataString += ',';
                } else {

                    if (!this.download) {
                        if (type === 'geojson') {
                            dataString += ']}';
                        }
                        dataString += '],';
                        var meta = {
                            cloneUrl: cloneUrl
                        };

                        dataString += `"meta": ${JSON.stringify(meta)} }`;
                    } else {
                        if (type === 'geojson') {
                            dataString += ']}';
                        }
                        dataString += ']}';
                    }
                }
                return dataString;
            }
        }
        *
        continue () {

            if (this.resultScroll[0].aggregations) {
                const data = csvSerializer.serialize(this.resultScroll, this.parsed, this.datasetId, this.format);
                this.stream.write(this.convertDataToDownload(data, this.format, true, false, this.cloneUrl), {
                    encoding: 'binary'
                });
            } else {
                this.first = true;
                while (!this.timeout && this.resultScroll[0].hits && this.resultScroll[0].hits && this.resultScroll[0].hits.hits.length > 0 && (this.total < this.limit || this.limit === -1)) {
                    logger.debug('Writting data');
                    let more = false;
                    const data = csvSerializer.serialize(this.resultScroll, this.parsed, this.datasetId, this.format);

                    this.total += this.resultScroll[0].hits.hits.length;
                    if (this.total < this.limit || this.limit === -1) {
                        this.resultScroll = yield this.elasticClient.getScroll({
                            scroll: '1m',
                            scroll_id: this.resultScroll[0]._scroll_id,
                        });
                        if (this.resultScroll[0].hits && this.resultScroll[0].hits && this.resultScroll[0].hits.hits.length > 0) {
                            more = true;
                        }
                    } else {
                        more = false;
                    }
                    this.stream.write(this.convertDataToDownload(data, this.format, this.first, more, this.cloneUrl), {
                        encoding: 'binary'
                    });
                    this.first = false;

                }
                if (this.total === 0) {
                    this.stream.write(this.convertDataToDownload(null, this.format, true, false, this.cloneUrl), {
                        encoding: 'binary'
                    });
                }
            }
            this.stream.end();
            if (this.timeout) {
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
            sql: function (opts) {
                return function (cb) {
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI('/_sql'),
                        body: opts.sql,
                        timeout: 60000,
                        requestTimeout: 60000,
                    }, cb);
                }.bind(this);
            },
            explain: function (opts) {
                return function (cb) {
                    var call = function (err, data) {
                        if (data) {
                            try {
                                data = JSON.parse(data);
                            } catch (e) {
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
            mapping: function (opts) {
                return function (cb) {
                    this.transport.request({
                        method: 'GET',
                        path: `${opts.index}/_mapping`
                    }, cb);
                }.bind(this);
            },
            delete: function (opts) {
                return function (cb) {
                    this.transport.request({
                        method: 'DELETE',
                        path: `${opts.index}`
                    }, cb);
                }.bind(this);
            },
            createScroll: function (opts) {
                return function (cb) {
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI(`${opts.index}/_search?scroll=${opts.duration}`),
                        body: JSON.stringify(opts.query)
                    }, cb);
                }.bind(this);
            },
            getScroll: function (opts) {
                logger.debug('GETSCROLL ', opts);
                return function (cb) {
                    this.transport.request({
                        method: 'GET',
                        path: encodeURI(`_search/scroll?scroll=${opts.scroll}&scroll_id=${opts.scroll_id}`),
                    }, cb);
                }.bind(this);
            },
            deleteByQuery: function (opts) {
                logger.debug('Delete by query ', opts);
                return function (cb) {
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI(`${opts.index}/_delete_by_query?slices=5&wait_for_completion=${opts.waitForCompletion ? 'true' : 'false'}`),
                        body: JSON.stringify(opts.body)
                    }, cb);
                }.bind(this);
            },
            update: function (opts) {
                logger.debug('Update element ', opts);
                return function (cb) {
                    this.transport.request({
                        method: 'POST',
                        path: encodeURI(`${opts.index}/${opts.type}/${opts.id}/_update`),
                        body: JSON.stringify(opts.body)
                    }, cb);
                }.bind(this);
            },
            ping: function (opts) {
                // logger.debug('ping ');
                return function (cb) {
                    this.transport.request({
                        method: 'GET',
                        path: ''
                    }, cb);
                }.bind(this);
            },
            getTask: function (opts) {
                logger.debug('get task ', opts);
                return function (cb) {
                    this.transport.request({
                        method: 'GET',
                        path: encodeURI(`_tasks/${opts.task}`)
                    }, cb);
                }.bind(this);
            },
            putSettings: function(opts) {
                logger.debug('put settings ', opts);
                return function (cb) {
                    this.transport.request({
                        method: 'PUT',
                        path: encodeURI(`${opts.index}/_settings`),
                        body: JSON.stringify(opts.body)
                    }, cb);
                }.bind(this);
            }
        };
        elasticsearch.Client.apis.sql = sqlAPI;

        this.elasticClient = new elasticsearch.Client({
            host: elasticUri,
            log: 'info',
            apiVersion: 'sql'
        });
        this.elasticClientV2 = new elasticsearch.Client({
            host: 'elasticsearch-v2.default.svc.cluster.local:9200',
            log: 'info',
            apiVersion: 'sql'
        });
        setInterval(() => {
            this.elasticClient.ping({
                // ping usually has a 3000ms timeout
                requestTimeout: 10000
            }, function (error) {
                if (error) {
                    logger.error('elasticsearch cluster is down!');
                    process.exit(1);
                }
            });
        }, 3000);

    }

    * updateElement(index, id, data) {
        logger.info(`Updating index ${index} and id ${id} with data`, data);
        try {
            const result = yield this.elasticClient.update({
                index,
                type: index,
                id: id,
                body: {
                    doc: data
                }
            });
            return result;
        } catch (err) {
            if (err && err.status === 404) {
                throw new DocumentNotFound(404, `Document with id ${id} not found`);
            }
            throw err;
        }
    }


    findIntersect(node, finded, result) {
        if (node && node.type === 'string' && node.value && finded) {
            try {
                logger.debug(node.value);
                if (node.value.startsWith('\'')) {
                    node.value = node.value.slice(1, node.value.length - 1);
                }

                const geojson = JSON.parse(node.value);

                const newResult = Object.assign({}, result || {}, {
                    geojson
                });

                return newResult;
            } catch (e) {
                logger.error(e);
                return result;
            }
        }
        if (node && node.type === 'number' && node.value && finded) {
            const newResult = Object.assign({}, result || {}, {
                wkid: node.value
            });
            return newResult;
        }
        if (node && node.type === 'function' && (node.value.toLowerCase() === 'st_intersects' || finded)) {
            for (let i = 0, length = node.arguments.length; i < length; i++) {
                const newResult = this.findIntersect(node.arguments[i], true, result);
                result = Object.assign(result || {}, newResult);
                return result;
            }
        }
        if (node && node.type === 'conditional') {
            const left = this.findIntersect(node.left);
            const right = this.findIntersect(node.right);
            if (left) {
                return left;
            } else if (right)  {
                return right;
            }
        }
        return null;
    }

    replaceIntersect(node, wkt) {
        if (node && node.type === 'function' && node.value.toLowerCase() === 'st_intersects') {
            return {
                type: 'function',
                value: 'GEO_INTERSECTS',
                arguments:  [{
                    type: 'literal',
                    value: 'the_geom'
                }, {
                    type: 'string',
                    value: `'${wkt}'`
                }]
            };
        }
        if (node && node.type === 'conditional') {
            const left = this.replaceIntersect(node.left, wkt);
            const right = this.replaceIntersect(node.right, wkt);
            node.left = left;
            node.right = right;
        }
        return node;
    }

    * convertQueryToElastic(parsed, index) {
        //search ST_GeoHash
        if (parsed.group || parsed.orderBy) {
            let mapping = yield this.getMapping(index);
            mapping = mapping[0][index].mappings.type ?  mapping[0][index].mappings.type.properties : mapping[0][index].mappings[index].properties;
            if (parsed.group) {
                
                for (let i = 0, length = parsed.group.length; i < length; i++) {
                    const node = parsed.group[i];
                    if (node.type === 'function' && node.value.toLowerCase() === 'st_geohash') {
                        const args = [];
                        args.push({
                            type: 'literal',
                            value: 'field=\'the_geom_point\'',
                        }, {
                            type: 'literal',
                            value: `precision=${node.arguments[1].value}`,
                        });
                        node.arguments = args;
                        node.value = 'geohash_grid';
                    } else if (node.type === 'literal') {
                        logger.debug('Checking if it is text');
                        logger.debug(mapping[node.value]);
                        const exists = parsed.select.find(sel => sel.alias === node.value);
                        if (exists) {
                            node.value = exists.value;
                        }
                        if (mapping[node.value] && mapping[node.value].type === 'text') {
                            node.value = `${node.value}.keyword`;
                        }

                    }
                }
            }
            if (parsed.orderBy) {
                for (let i = 0, length = parsed.orderBy.length; i < length; i++) {
                    const node = parsed.orderBy[i];
                    if (node.type === 'literal') {
                        logger.debug('Checking if it is text');
                        logger.debug(mapping[node.value]);
                        if (mapping[node.value] && mapping[node.value].type === 'text') {
                            node.value = `${node.value}.keyword`;
                        }
                    }
                }
            }
            if (parsed.select) {
                for (let i = 0, length = parsed.select.length; i < length; i++) {
                    const node = parsed.select[i];
                    if (node.type === 'function') {
                        for (let j = 0; j < node.arguments.length; j++) {
                            if (node.arguments[j].type === 'literal' && mapping[node.arguments[j].value] && mapping[node.arguments[j].value].type === 'text') {
                                node.arguments[j].value = `${node.arguments[j].value}.keyword`;
                            }
                        }
                    }
                }
            }

            if (!parsed.limit) {
                parsed.limit = 9999999; // in group by is need it because elastic has a 10000 limit by default
            }
        }
        if (parsed.select) {
            let mapping = yield this.getMapping(index);
            for (let i = 0, length = parsed.select.length; i < length; i++) {
                const node = parsed.select[i];
                if (node.type === 'function') {
                    if (node.value.toLowerCase() === 'st_geohash')  {
                        const args = [];
                        args.push({
                            type: 'literal',
                            value: 'field=\'the_geom_point\'',
                        }, {
                            type: 'literal',
                            value: `precision=${node.arguments[1].value}`,
                        });
                        node.arguments = args;
                        node.value = 'geohash_grid';
                    }
                    for (let j = 0; j < node.arguments.length; j++) {
                        if (node.arguments[j].type === 'literal' && mapping[node.arguments[j].value] && mapping[node.arguments[j].value].type === 'text') {
                            node.arguments[j].value = `${node.arguments[j].value}.keyword`;
                        }
                    }
                }
            }
        }
        logger.debug('parsed', parsed);
        const geo = this.findIntersect(parsed.where);
        logger.debug('find intersect', geo);
        if (geo) {
            const wkt = Terraformer.convert(geo.geojson);
            parsed.where = this.replaceIntersect(parsed.where, wkt);
        }
        return parsed;
    }

    * doQuery(sql, parsed, index, datasetId, body, cloneUrl, format) {
        logger.info('Doing query...');
        parsed = yield this.convertQueryToElastic(parsed, index);
        let removeAlias = Object.assign({}, parsed);
        if (removeAlias.select) {
            removeAlias.select = removeAlias.select.map(el => {
                if (el.type === 'function') {
                    return el;
                }
                return {
                    value: el.value,
                    type: el.type,
                    alias: null,
                    arguments: el.arguments
                };
            });
        }
        sql = Json2sql.toSQL(removeAlias);
        logger.debug('sql', sql);

        var scroll = new Scroll(this.elasticClient, sql, parsed, index, datasetId, body, false, cloneUrl, format);
        yield scroll.init();
        yield scroll.continue();
        logger.info('Finished query');
    }

    * doQueryV2(sql, parsed, index, datasetId, body, cloneUrl, format) {
        logger.info('Doing query...');
        parsed = yield this.convertQueryToElastic(parsed, index);
        let removeAlias = Object.assign({}, parsed);
        if (removeAlias.select) {
            removeAlias.select = removeAlias.select.map(el => {
                if (el.type === 'function') {
                    return el;
                }
                return {
                    value: el.value,
                    type: el.type,
                    alias: null,
                    arguments: el.arguments
                };
            });
        }
        sql = Json2sql.toSQL(removeAlias);
        logger.debug('sql', sql);

        var scroll = new Scroll(this.elasticClientV2, sql, parsed, index, datasetId, body, false, cloneUrl, format);
        yield scroll.init();
        yield scroll.continue();
        logger.info('Finished query');
    }

    * activateRefreshIndex(index) {
        let options = {
            index: index,
            body: {
                index: {
                    refresh_interval: '1s',
                    number_of_replicas: 1
                }
            }
        };
        yield this.elasticClient.putSettings(options);
    }

    * desactivateRefreshIndex(index) {
        let options = {
            index: index,
            body: {
                index: {
                    refresh_interval: '-1',
                    number_of_replicas: 0
                }
            }
        };
        yield this.elasticClient.putSettings(options);

    }

    *
    updateState(id, status, errorMessage) {
        logger.info('Updating state of dataset ', id, ' with status ', status);

        let options = {
            uri: '/dataset/' + id,
            body: {
               status
            },
            method: 'PATCH',
            json: true
        };

        if (errorMessage) {
            options.body.errorMessage = errorMessage;
        }
        // logger.info('Updating', options);
        try {
            let result = yield ctRegisterMicroservice.requestToMicroservice(options);
        } catch (e) {
            logger.error(e);
            throw new Error('Error to updating dataset');
        }
    }

    * doDeleteQuery(id, sql, parsed, tableName) {
        try {
            logger.info(`Doing delete to ${sql}`);
            logger.debug('Obtaining explain with select ', `${sql}`);
            yield this.updateState(id, 0, null);
            yield this.desactivateRefreshIndex(tableName);
            parsed = yield this.convertQueryToElastic(parsed, tableName);
            parsed.select = [{
                value: '*',
                alias: null,
                type: 'wildcard'
            }];
            delete parsed.delete;
            // logger.debug('sql', sql);
            sql = Json2sql.toSQL(parsed);
            try {
                let resultQueryElastic = yield this.elasticClient.explain({
                    sql
                });
                delete resultQueryElastic.from;
                delete resultQueryElastic.size;
                logger.debug('Doing query');
                let result = yield this.elasticClient.deleteByQuery({
                    index: tableName,
                    timeout: 120000,
                    requestTimeout: 120000,
                    body: resultQueryElastic,

                    waitForCompletion: false
                });
                logger.debug(result);
                const interval = setInterval(() => {
                    co(function *() {
                        try {
                            logger.info('Checking task');
                            const data = yield this.elasticClient.getTask({task: result[0].task});
                            // logger.debug('data', data);
                            if (data && data.length > 0 && data[0].completed) {
                                logger.info(`Dataset ${id} is completed`);
                                clearInterval(interval);
                                yield this.updateState(id, 1, null);
                                yield this.activateRefreshIndex(tableName);
                            }
                        } catch(err){
                            clearInterval(interval);
                            yield this.updateState(id, 2, null);
                            yield this.activateRefreshIndex(tableName);
                        }
                    }.bind(this)).then(() => {

                    }, err => {
                        logger.error('Error checking task', err);

                    });

                }, 2000);
                return '';
            } catch (e) {
                logger.error(e);
                throw new Error('Query not valid');
            }
        } catch(err) {
            logger.error('Error removing query', err);
            yield this.updateState(id, 2, 'Error deleting items');
        }

    }

    * downloadQuery(sql, parsed, index, datasetId, body, type = 'json') {
        logger.info('Download with query...');
        parsed = yield this.convertQueryToElastic(parsed, index);
        logger.debug('sql', sql);
        sql = Json2sql.toSQL(parsed);
        var scroll = new Scroll(this.elasticClient, sql, parsed, index, datasetId, body, true, null, type);
        yield scroll.init();
        yield scroll.continue();
        logger.info('Finished query');
    }

    * getMapping(index) {
        logger.info('Obtaining mapping...');

        let result = yield this.elasticClient.mapping({
            index: index
        });
        return result;
    }

    * deleteIndex(index) {
        logger.info('Deleting index %s...', index);

        let result = yield this.elasticClient.delete({
            index: index
        });
        return result;
    }
}

module.exports = new QueryService();
