'use strict';

var Router = require('koa-router');
var logger = require('logger');
var config = require('config');
var importerService = require('services/importerService');
var queryService = require('services/queryService');
var csvSerializer = require('serializers/csvSerializer');
var fieldSerializer = require('serializers/fieldSerializer');
var redisClient = require('redis').createClient({port: config.get('redis.port'), host:config.get('redis.host')});
var router = new Router({
    prefix: '/csv'
});


class CSVRouter {

    static * import () {
        logger.info('Adding csv with dataset id: ', this.request.body.connector);
        yield importerService.addCSV(this.request.body.connector.connector_url, 'index-' + this.request.body.connector.id, this.request.body.connector.id);
        this.body = '';
    }

    static * query() {
        logger.info('Do Query with dataset', this.request.body);

        let result = yield queryService.doQuery(this.query.select, this.query.order,
            this.query.aggr_by, this.query.filter, this.query.filter_not, this.query.limit, this.query.aggr_columns, 'index-' + this.request.body.dataset.id, this.query.sql);
        let data = csvSerializer.serialize(result);
        let fields = yield queryService.getMapping('index-' + this.request.body.dataset.id);
        data.fields = fieldSerializer.serialize(fields);
        data.clone_url = CSVRouter.getCloneUrl(this.request.url, this.params.dataset);
        this.body = data;
    }

    static * query() {
        logger.debug(this.query);
        logger.info('Get fields of dataset', this.request.body);

        let fields = yield queryService.getMapping('index-' + this.request.body.dataset.id);
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
        let result = yield importerService.deleteCSV('index-' + this.params.id, this.params.id);
        this.body = result;
    }
}

const cacheMiddleware = function*(next){
    let data = yield redisClient.getAsync(this.request.url);
    if(data){
        this.body = data;
        return;
    }
    yield next;
    // save result
    logger.info('Caching data');
    redisClient.setex(this.request.url, config.get('timeCache'), JSON.stringify(this.body));

};

router.post('/query/:dataset', cacheMiddleware, CSVRouter.query);
router.post('/fields/:dataset', cacheMiddleware, CSVRouter.fields);
router.post('/', CSVRouter.import);
router.delete('/:id', CSVRouter.delete);

module.exports = router;
