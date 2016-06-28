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


class TaskRouter {

    static * import(){
        logger.info('Adding csv with dataset id: ', this.request.body.connector);
        yield importerService.addTask(this.request.body.connector.connector_url, this.request.body.connector.attributes_path, this.request.body.connector.id);
        this.body = '';
    }

    static * query(){
        logger.info('Do Query with dataset', this.request.body);
        logger.debug('limit', this.query.select);
        let result = yield queryService.doQuery(this.query.select, this.query.order,
            this.query.aggrBy, this.query.filter, this.query.filterNot, this.query.limit, this.request.body.dataset, this.query.sql);
        this.body = csvSerializer.serialize(result);
    }

    static * mapping(){
        logger.debug('Mapping');
        let result = yield queryService.getMapping();
        this.body = result;
    }

    static * delete(){
        logger.info('Deleting dataset');
    }
}

router.post('/query/:dataset', TaskRouter.query);
router.post('/mapping/:dataset', TaskRouter.mapping);
router.post('/', TaskRouter.import);
router.delete('/:id', TaskRouter.delete);

module.exports = router;
