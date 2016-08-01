'use strict';

var Router = require('koa-router');
var logger = require('logger');
var config = require('config');
var importerService = require('services/importerService');
var queryService = require('services/queryService');
var csvSerializer = require('serializers/csvSerializer');
var fieldSerializer = require('serializers/fieldSerializer');
var redisClient = require('redis').createClient({
    port: config.get('redis.port'),
    host: config.get('redis.host')
});
var router = new Router({
    prefix: '/csv'
});


class CSVRouter {

    static *
        import () {
            logger.info('Adding csv with dataset id: ', this.request.body.connector);
            yield importerService.addCSV(this.request.body.connector.connector_url, 'index_' + this.request.body.connector.id.replace(/-/g, ''), this.request.body.connector.id);
            this.body = '';
        }

    static * query() {
        logger.info('Do Query with dataset', this.request.body);

        let result = yield queryService.doQuery( this.query.sql);
        let data = csvSerializer.serialize(result, this.query.sql);
        let fields = yield queryService.getMapping(this.request.body.dataset.table_name);
        data.fields = fieldSerializer.serialize(fields);
        data.clone_url = CSVRouter.getCloneUrl(this.request.url, this.params.dataset);
        this.body = data;
    }

    static * fields() {
        logger.info('Get fields of dataset', this.request.body);

        let fields = yield queryService.getMapping(this.request.body.dataset.table_name);
        this.body = fieldSerializer.serialize(fields);
    }

    static getCloneUrl(url, idDataset) {
        return {
            http_method: 'POST',
            url: `/datasets/${idDataset}/clone`,
            body: {
                dataset: {
                    dataset_url: url.replace('/csv', '')
                }
            }
        };
    }
    static * delete() {
        logger.info('Deleting index with dataset', this.request.body);
        let result = yield importerService.deleteCSV('index_' + this.params.id.replace(/-/g, ''), this.params.id);
        this.body = result;
    }
}

const cacheMiddleware = function*(next) {
    let data = yield redisClient.getAsync(this.request.url);
    if (data) {
        this.body = data;
        return;
    }
    yield next;
    // save result
    logger.info('Caching data');
    redisClient.setex(this.request.url, config.get('timeCache'), JSON.stringify(this.body));

};

const toSQLMiddleware = function*(next) {
    let microserviceClient = require('vizz.microservice-client');
    let options = {
        method: 'GET',
        json: true
    };

    if (this.query.sql) {
        logger.debug('Checking sql correct');
        options.uri = '/convert/checkSQL?sql=' + this.query.sql;
    } else {
        logger.debug('Obtaining sql from featureService');
        if(this.request.search){
            options.uri = '/convert/fs2SQL' + this.request.search + '&tableName=' + this.request.body.dataset.table_name;
        } else {
            options.uri = '/convert/fs2SQL?tableName=' + this.request.body.dataset.table_name;
        }
    }
    logger.debug(options);
    try {
        let result = yield microserviceClient.requestToMicroservice(options);
        if (result.statusCode === 204 || result.statusCode === 200) {
            if (!this.query.sql) {
                this.query.sql = result.body.data.attributes.sql;
            }
            logger.debug('Doing query with sql: ', this.query.sql);
            yield next;
        } else {
            if(result.statusCode === 400){
                this.status = result.statusCode;
                this.body = result.body;
            } else {
                this.throw(result.statusCode, result.body);
            }
        }
    } catch (e) {
        if (e.errors && e.errors.length > 0 && e.errors[0].status >= 400 && e.errors[0].status < 500) {
            this.status = e.errors[0].status;
            this.body = e;
        } else {
            throw e;
        }
    }
};

router.post('/query/:dataset', cacheMiddleware, toSQLMiddleware, CSVRouter.query);
router.post('/fields/:dataset', cacheMiddleware, CSVRouter.fields);
router.post('/', CSVRouter.import);
router.delete('/:id', CSVRouter.delete);

module.exports = router;
