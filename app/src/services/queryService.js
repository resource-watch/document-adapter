const logger = require('logger');
const config = require('config');
const elasticsearch = require('@elastic/elasticsearch');
const Scroll = require('services/scroll');
const Json2sql = require('sql2json').json2sql;
const Terraformer = require('terraformer-wkt-parser');

const elasticUri = config.get('elasticsearch.host');

class QueryService {

    constructor() {
        logger.info(`Connecting to Elasticsearch at ${elasticUri}`);

        const elasticSearchConfig = {
            node: elasticUri,
            log: 'info',
            apiVersion: 'sql'
        };

        if (config.get('elasticsearch.user') && config.get('elasticsearch.password')) {
            elasticSearchConfig.auth = {
                username: config.get('elasticsearch.user'),
                password: config.get('elasticsearch.password')
            };
        }

        this.elasticClient = new elasticsearch.Client(elasticSearchConfig);

        this.elasticClient.extend('opendistro.explain', ({ makeRequest, ConfigurationError }) => function explain(params, options = {}) {
            const {
                body,
                index,
                method,
                ...querystring
            } = params;

            // params validation
            if (body == null) {
                throw new ConfigurationError('Missing required parameter: body');
            }

            // build request object
            const request = {
                method: method || 'POST',
                path: `/_opendistro/_sql/_explain`,
                body,
                querystring
            };

            // build request options object
            const requestOptions = {
                ignore: options.ignore || null,
                requestTimeout: options.requestTimeout || null,
                maxRetries: options.maxRetries || null,
                asStream: options.asStream || false,
                headers: options.headers || null
            };

            return makeRequest(request, requestOptions);
        });

        this.elasticClient.extend('opendistro.query', ({ makeRequest, ConfigurationError }) => function query(params, options = {}) {
            const {
                body,
                index,
                method,
                ...querystring
            } = params;

            // params validation
            if (body == null) {
                throw new ConfigurationError('Missing required parameter: body');
            }

            // build request object
            const request = {
                method: method || 'POST',
                path: `/_opendistro/_sql`,
                body,
                querystring
            };

            // build request options object
            const requestOptions = {
                ignore: options.ignore || null,
                requestTimeout: options.requestTimeout || null,
                maxRetries: options.maxRetries || null,
                asStream: options.asStream || false,
                headers: options.headers || null
            };

            return makeRequest(request, requestOptions);
        });

        setInterval(() => {
            this.elasticClient.ping({}, (error) => {
                if (error) {
                    logger.error('Elasticsearch cluster is down!');
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

                const newResult = { ...result || {}, geojson };

                return newResult;
            } catch (e) {
                logger.error(e);
                return result;
            }
        }
        if (node && node.type === 'number' && node.value && finded) {
            const newResult = { ...result || {}, wkid: node.value };
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
            const mapping = await this.getMapping(index);
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
                        const exists = parsed.select.find((sel) => sel.alias === node.value);
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
        logger.info('[QueryService - doQuery] Doing query...');
        const elasticQuery = await this.convertQueryToElastic(parsed, index);
        const removeAlias = { ...elasticQuery };
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
        logger.debug('[QueryService - doQuery] doQuery - Generated sql', sqlFromJson);

        const scroll = new Scroll(this.elasticClient, sqlFromJson, elasticQuery, index, datasetId, body, false, cloneUrl, format);
        await scroll.init();
        await scroll.continue();
        logger.info('[QueryService - doQuery] Finished query');
    }

    async downloadQuery(sql, parsed, index, datasetId, body, type = 'json') {
        logger.info('[QueryService - downloadQuery] Download with query...');
        const elasticQuery = await this.convertQueryToElastic(parsed, index);
        logger.debug('[QueryService - downloadQuery] Download query sql: ', sql);
        const sqlFromJson = Json2sql.toSQL(elasticQuery);
        const scroll = new Scroll(this.elasticClient, sqlFromJson, elasticQuery, index, datasetId, body, true, null, type);
        await scroll.init();
        await scroll.continue();
        logger.info('[QueryService - downloadQuery] Finished query');
    }

    async getMapping(index) {
        logger.info('[QueryService - getMapping] Obtaining mapping...');

        const mappingResponse = await this.elasticClient.indices.getMapping({ index });
        return mappingResponse.body[index].mappings.properties;
    }

}

module.exports = new QueryService();
