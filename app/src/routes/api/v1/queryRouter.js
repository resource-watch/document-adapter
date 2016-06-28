'use strict';

var Router = require('koa-router');
var logger = require('logger');
var config = require('config');
var importerService = require('services/importerService');
var queryService = require('services/queryService');
var csvSerializer = require('serializers/csvSerializer');
var router = new Router({
    prefix: '/csv'
});


class CSVRouter {

    static * import(){
        logger.info('Adding csv with dataset id: ', this.request.body.connector);
        yield importerService.addTask(this.request.body.connector.connector_url, this.request.body.connector.attributes_path, this.request.body.connector.id);
        this.body = '';
    }

    static * query(){
        logger.info('Do Query with dataset', this.request.body);

        let result = yield queryService.doQuery(this.query.select, this.query.order,
            this.query.aggrBy, this.query.filter, this.query.filterNot, this.query.limit, this.request.body.dataset.attributes_path, this.query.sql);
        this.body = csvSerializer.serialize(result);
    }

    static * mapping(){
        logger.info('Obtaining mapping with dataset', this.request.body);
        let result = yield queryService.getMapping(this.request.body.dataset.attributes_path);
        this.body = result;
    }

    static * delete(){
        logger.info('Deleting index with dataset', this.request.body);
        let result = yield queryService.deleteIndex(this.params.index);
        this.body = result;
    }
}

router.post('/query/:dataset', CSVRouter.query);
router.post('/mapping/:dataset', CSVRouter.mapping);
router.post('/', CSVRouter.import);
router.delete('/:index', CSVRouter.delete);

module.exports = router;
