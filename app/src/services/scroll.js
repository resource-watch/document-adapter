const logger = require('logger');
const json2csv = require('json2csv');
const csvSerializer = require('serializers/csvSerializer');
const IndexNotFound = require('errors/indexNotFound');

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
            resultQueryElastic = translatedQuery.body;
        } catch (e) {
            if (e.message.includes('index_out_of_bounds_exception')) {
                throw new Error('Semantically invalid query', e);
            }

            throw e;
        }

        // if (this.parsed.group) {
        //     logger.debug('Config size of aggregations');
        //     let aggregations = { resultQueryElastic };
        //     while (aggregations) {
        //         const keys = Object.keys(aggregations);
        //         if (keys.length === 1) {
        //             if (aggregations[keys[0]] && aggregations[keys[0]].terms) {
        //                 aggregations[keys[0]].terms.size = this.parsed.limit || 999999;
        //                 // eslint-disable-next-line prefer-destructuring
        //                 aggregations = aggregations[keys[0]].aggregations;
        //             } else if (keys[0].indexOf('NESTED') >= -1) {
        //                 // eslint-disable-next-line prefer-destructuring
        //                 aggregations = aggregations[keys[0]].aggregations;
        //             } else {
        //                 aggregations = null;
        //             }
        //         } else {
        //             aggregations = null;
        //         }
        //     }
        // }
        // this.limit = -1;
        // if (this.sql.toLowerCase().indexOf('limit') >= 0) {
        //     this.limit = resultQueryElastic.size;
        // }
        //
        // if (resultQueryElastic.size > 10000 || this.limit === -1) {
        //     resultQueryElastic.size = 10000;
        // }
        // logger.debug('Creating params to scroll with query', resultQueryElastic);

        if (resultQueryElastic.sort) {
            const sort = resultQueryElastic.sort.map((element) => {
                // const result = {};
                // result[Object.keys(element)[0]] = element[Object.keys(element)[0]].order;
                // return result;
                element[Object.keys(element)[0]].unmapped_type = 'long';
                element[Object.keys(element)[0]].missing = '_last';
                return element;
            });
            resultQueryElastic.sort = sort;

        }

        // if (resultQueryElastic.aggregations) {
        //     resultQueryElastic.aggs = resultQueryElastic.aggregations;
        //     delete resultQueryElastic.aggregations;
        // }

        try {
            logger.debug('Creating scroll');
            const searchResult = await this.elasticClient.search({
                // ...resultQueryElastic,
                scroll: '1m',
                index: this.index,
                q: resultQueryElastic
            });
            this.resultScroll = searchResult.body;
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
                dataString = JSON.stringify(data);
                dataString = dataString.substring(9, dataString.length - 2); // remove {"data": [ and ]}
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
            const data = csvSerializer.serialize(this.resultScroll, this.parsed, this.datasetId, this.format);
            this.stream.write(this.convertDataToDownload(data, this.format, true, false, this.cloneUrl), {
                encoding: 'binary'
            });
        } else {
            this.first = true;
            while (!this.timeout && this.resultScroll.hits && this.resultScroll.hits && this.resultScroll.hits.hits.length > 0 && (this.total < this.limit || this.limit === -1)) {
                logger.debug('Writting data');
                let more = false;
                const data = csvSerializer.serialize(this.resultScroll, this.parsed, this.datasetId, this.format);

                this.total += this.resultScroll.hits.hits.length;
                if (this.total < this.limit || this.limit === -1) {
                    this.resultScroll = await this.elasticClient.getScroll({
                        scroll: '1m',
                        // eslint-disable-next-line no-underscore-dangle
                        scroll_id: this.resultScroll._scroll_id,
                    });
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

}

module.exports = Scroll;
