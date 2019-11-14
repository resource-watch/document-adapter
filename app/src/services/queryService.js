const logger = require('logger');
const config = require('config');
const elasticsearch = require('@elastic/elasticsearch');
const Scroll = require('services/scroll');
const Json2sql = require('sql2json').json2sql;
const Terraformer = require('terraformer-wkt-parser');

const elasticUri = process.env.ELASTIC_URI || `${config.get('elasticsearch.host')}:${config.get('elasticsearch.port')}`;


class QueryService {

    constructor() {
        logger.info('Connecting with elasticsearch');

        const sqlAPI = {
            async sql(opts) {
                this.transport.requestTimeout = 60000;
                return this.transport.request({
                    method: 'POST',
                    path: encodeURI('/_sql'),
                    body: opts.sql
                });
            },
            async explain(opts) {
                const response = await this.transport.request({
                    method: 'POST',
                    path: encodeURI('/_sql/_explain'),
                    body: opts.sql
                });

                try {
                    return JSON.parse(response.body);
                } catch (e) {
                    return null;
                }
            },
            async mapping(opts) {
                return this.transport.request({
                    method: 'GET',
                    path: `/${opts.index}/_mapping`
                });
            },
            async createScroll(opts) {
                this.transport.requestTimeout = 60000;
                const response = await this.transport.request({
                    method: 'POST',
                    path: encodeURI(`/${opts.index}/_search?scroll=${opts.duration}`),
                    body: JSON.stringify(opts.query),
                    requestTimeout: 60000
                });

                return response.body;
            },
            async getScroll(opts) {
                logger.debug('GETSCROLL ', opts);
                this.transport.requestTimeout = 60000;
                const response = await this.transport.request({
                    method: 'GET',
                    path: encodeURI(`/_search/scroll?scroll=${opts.scroll}&scroll_id=${opts.scroll_id}`),
                    requestTimeout: 60000
                });
                return response.body;
            },
            async ping() {
                return this.transport.request({
                    method: 'GET',
                    path: ''
                });
            }
        };

        this.elasticClient = new elasticsearch.Client({
            node: `http://${elasticUri}`,
            log: 'info',
            apiVersion: 'sql'
        });
        this.elasticClientV2 = new elasticsearch.Client({
            node: 'http://elasticsearch-v2.default.svc.cluster.local:9200',
            log: 'info',
            apiVersion: 'sql'
        });

        this.elasticClient = Object.assign(this.elasticClient, sqlAPI);
        this.elasticClientV2 = Object.assign(this.elasticClientV2, sqlAPI);

        setInterval(() => {
            this.elasticClient.ping({}, (error) => {
                if (error) {
                    logger.error('elasticsearch cluster is down!');
                    process.exit(1);
                }
            });
        }, 3000);

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
            for (let i = 0, { length } = node.arguments; i < length; i += 1) {
                const newResult = this.findIntersect(node.arguments[i], true, result);
                // eslint-disable-next-line no-param-reassign
                result = Object.assign(result || {}, newResult);
                return result;
            }
        }
        if (node && node.type === 'conditional') {
            const left = this.findIntersect(node.left);
            const right = this.findIntersect(node.right);
            if (left) {
                return left;
            }
            if (right) {
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
                arguments: [{
                    type: 'literal',
                    value: 'the_geom'
                }, {
                    type: 'string',
                    value: `${wkt}`
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

    async convertQueryToElastic(parsed, index) {
        // search ST_GeoHash
        if (parsed.group || parsed.orderBy) {
            let mapping = await this.getMapping(index);
            mapping = mapping.body[index].mappings.type ? mapping.body[index].mappings.type.properties : mapping.body[index].mappings.properties;
            if (parsed.group) {

                for (let i = 0, { length } = parsed.group; i < length; i += 1) {
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
                for (let i = 0, { length } = parsed.orderBy; i < length; i += 1) {
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
                for (let i = 0, { length } = parsed.select; i < length; i += 1) {
                    const node = parsed.select[i];
                    if (node.type === 'function') {
                        for (let j = 0; j < node.arguments.length; j += 1) {
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
            const mapping = await this.getMapping(index);
            for (let i = 0, { length } = parsed.select; i < length; i += 1) {
                const node = parsed.select[i];
                if (node.type === 'function') {
                    if (node.value.toLowerCase() === 'st_geohash') {
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
                    for (let j = 0; j < node.arguments.length; j += 1) {
                        if (node.arguments[j].type === 'literal' && mapping[node.arguments[j].value] && mapping[node.arguments[j].value].type === 'text') {
                            node.arguments[j].value = `${node.arguments[j].value}.keyword`;
                        }
                    }
                }
            }
        }
        logger.debug('convertQueryToElastic - Parsed', parsed);
        const geo = this.findIntersect(parsed.where);
        logger.debug('convertQueryToElastic - Intersection found:', geo);
        if (geo) {
            const wkt = Terraformer.convert(geo.geojson);
            parsed.where = this.replaceIntersect(parsed.where, wkt);
        }
        return parsed;
    }

    async doQuery(sql, parsed, index, datasetId, body, cloneUrl, format) {
        logger.info('Doing query...');
        const elasticQuery = await this.convertQueryToElastic(parsed, index);
        const removeAlias = Object.assign({}, elasticQuery);
        if (removeAlias.select) {
            removeAlias.select = removeAlias.select.map((el) => {
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
        const sqlFromJson = Json2sql.toSQL(removeAlias);
        logger.debug('doQuery - Generated sql', sqlFromJson);

        const scroll = new Scroll(this.elasticClient, sqlFromJson, elasticQuery, index, datasetId, body, false, cloneUrl, format);
        await scroll.init();
        await scroll.continue();
        logger.info('Finished query');
    }

    async doQueryV2(sql, parsed, index, datasetId, body, cloneUrl, format) {
        logger.info('Doing query...');
        const elasticQuery = await this.convertQueryToElastic(parsed, index);
        const removeAlias = Object.assign({}, elasticQuery);
        if (removeAlias.select) {
            removeAlias.select = removeAlias.select.map((el) => {
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
        const sqlFromJson = Json2sql.toSQL(removeAlias);
        logger.debug('doQueryV2 - sql', sql);

        const scroll = new Scroll(this.elasticClientV2, sqlFromJson, elasticQuery, index, datasetId, body, false, cloneUrl, format);
        await scroll.init();
        await scroll.continue();
        logger.info('Finished query');
    }

    async downloadQuery(sql, parsed, index, datasetId, body, type = 'json') {
        logger.info('Download with query...');
        const elasticQuery = await this.convertQueryToElastic(parsed, index);
        logger.debug('Download query sql: ', sql);
        const sqlFromJson = Json2sql.toSQL(elasticQuery);
        const scroll = new Scroll(this.elasticClient, sqlFromJson, elasticQuery, index, datasetId, body, true, null, type);
        await scroll.init();
        await scroll.continue();
        logger.info('Finished query');
    }

    async getMapping(index) {
        logger.info('Obtaining mapping...');

        return this.elasticClient.mapping({ index });
    }

}

module.exports = new QueryService();
