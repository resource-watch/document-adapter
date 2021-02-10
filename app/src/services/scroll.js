const logger = require('logger');
const json2csv = require('json2csv');
const CSVSerializer = require('serializers/csvSerializer');
const JSONSerializer = require('serializers/jsonSerializer');
const IndexNotFound = require('errors/indexNotFound');
const InvalidQueryError = require('errors/invalidQuery.error');
const TooManyBucketsError = require('errors/tooManyBuckets.error');

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

    async init() {
        logger.debug('Scroll init');
        this.timeoutFunc = setTimeout(() => {
            this.timeout = true;
        }, 60000);

        let resultQueryElastic;
        try {
            logger.debug('Scroll init - Query explain: ', this.sql);
            const translatedQuery = await this.elasticClient.opendistro.explain({
                body: {
                    query: this.sql
                }
            });

            if (typeof translatedQuery.body === 'string') {
                resultQueryElastic = JSON.parse(translatedQuery.body);
            } else {
                resultQueryElastic = translatedQuery.body;
            }
        } catch (e) {
            if (e.message.includes('index_out_of_bounds_exception')) {
                throw new Error('Semantically invalid query', e);
            }

            let jsonErrorMessage = null;
            if (e.meta && e.meta.body) {
                try {
                    jsonErrorMessage = JSON.parse(e.meta.body);
                    jsonErrorMessage = jsonErrorMessage.error.details;
                } catch (jsonParsingError) {
                    logger.debug(`Could not convert ${e.meta.body} error to JSON, proceeding as string`);
                }

                throw new InvalidQueryError(400, jsonErrorMessage);
            }

            throw e;
        }

        if (this.parsed.group) {
            logger.debug('Config size of aggregations');
            let aggregations = { resultQueryElastic };
            while (aggregations) {
                const keys = Object.keys(aggregations);
                if (keys.length === 1) {
                    if (aggregations[keys[0]] && aggregations[keys[0]].terms) {
                        aggregations[keys[0]].terms.size = this.parsed.limit || 999999;
                        // eslint-disable-next-line prefer-destructuring
                        aggregations = aggregations[keys[0]].aggregations;
                    } else if (keys[0].indexOf('NESTED') >= -1) {
                        // eslint-disable-next-line prefer-destructuring
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
        if (this.sql.toLowerCase().indexOf('limit') >= 0 && resultQueryElastic.size !== 0) {
            this.limit = resultQueryElastic.size;
        }

        if (resultQueryElastic.size > 10000 || this.limit === -1) {
            resultQueryElastic.size = 10000;
        }
        logger.debug('Creating params to scroll with query', resultQueryElastic);

        try {
            logger.debug('Creating scroll');

            // Removing from, because since ES > 6.X.X it throws a validation error
            if (resultQueryElastic.from !== undefined) {
                delete resultQueryElastic.from;
            }

            // Removing size if 0, because since ES > 6.X.X it throws a validation error
            if (resultQueryElastic.size === 0) {
                delete resultQueryElastic.size;
            }

            const searchResult = await this.elasticClient.search({
                scroll: '1m',
                index: this.index,
                body: resultQueryElastic,
                method: 'POST'
            });
            this.resultScroll = searchResult.body;
            this.first = true;
            this.total = 0;

        } catch (err) {
            logger.warn(`Error creating scroll. Status code: ${err.statusCode} | Response: ${JSON.stringify(err.body)}`);
            if (err.statusCode === 404) {
                throw new IndexNotFound(404, 'Table not found');
            }
            if (err.statusCode === 400) {
                throw new InvalidQueryError(400, err.body.error.root_cause[0].reason);
            }
            if (err.meta.body.error.caused_by.type === 'too_many_buckets_exception') {
                throw new TooManyBucketsError(400, 'Your are using a "group by" query that produces too many results. Please reduce the number of rows your "group by" query produces (ie. more restrictive "where" clause or use less "group by" criteria)');
            }
            throw err;
        }
    }

    convertDataToDownload(data, type, first, more, cloneUrl) {
        if (type === 'csv') {
            if (data.data.length === 0) {
                return '';
            }
            return `${json2csv({
                data: data ? data.data : [],
                hasCSVColumnTitle: first
            })}\n`;
        }
        if (type === 'json' || type === 'geojson') {
            let dataString = '';
            if (data) {
                // TODO: Evaluate if equivalent to dataString = dataString.substring(9, dataString.length - 2);
                // remove {"data": [ and ]}
                dataString = JSON.stringify(data);
                dataString = dataString.substring(9, dataString.length - 2);
            }
            if (first) {
                if (type === 'geojson') {
                    dataString = `{"data":[{"type": "FeatureCollection", "features": [${dataString}`;
                } else {
                    dataString = `{"data":[${dataString}`;
                }
            }
            if (more) {
                dataString += ',';
            } else if (!this.download) {
                if (type === 'geojson') {
                    dataString += ']}';
                }
                dataString += '],';
                const meta = {
                    cloneUrl
                };

                dataString += `"meta": ${JSON.stringify(meta)} }`;
            } else {
                if (type === 'geojson') {
                    dataString += ']}';
                }
                dataString += ']}';
            }
            return dataString;
        }
        return null;
    }

    async continue() {

        if (this.resultScroll.aggregations) {
            const data = Scroll.serialize(this.resultScroll, this.parsed, this.format);
            this.stream.write(this.convertDataToDownload(data, this.format, true, false, this.cloneUrl), {
                encoding: 'binary'
            });
        } else {
            this.first = true;
            while (!this.timeout && this.resultScroll.hits && this.resultScroll.hits.hits && this.resultScroll.hits.hits.length > 0 && (this.total < this.limit || this.limit === -1)) {
                logger.debug('[Scroll - continue] Writing data');
                let more = false;
                const data = Scroll.serialize(this.resultScroll, this.parsed, this.format);

                this.total += this.resultScroll.hits.hits.length;
                if (this.total < this.limit || this.limit === -1) {
                    const searchResult = await this.elasticClient.scroll({
                        scroll: '1m',
                        // eslint-disable-next-line no-underscore-dangle
                        scrollId: this.resultScroll._scroll_id,
                    });
                    this.resultScroll = searchResult.body;
                    if (this.resultScroll.hits && this.resultScroll.hits && this.resultScroll.hits.hits.length > 0) {
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

    async clear() {
        logger.info('Clearing scroll...');
        return this.elasticClient.clearScroll({
            // eslint-disable-next-line no-underscore-dangle
            body: { scroll_id: [this.resultScroll._scroll_id] },
        });
    }

    static serialize(resultScroll, parsed, format) {
        switch (format) {

            case 'json':
            case 'geojson':
                return JSONSerializer.serialize(resultScroll, parsed, format);
            case 'csv':
            default:
                return CSVSerializer.serialize(resultScroll, parsed);

        }

    }

}

module.exports = Scroll;
