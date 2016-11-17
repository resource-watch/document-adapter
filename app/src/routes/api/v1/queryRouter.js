'use strict';

var Router = require('koa-router');
var logger = require('logger');
var config = require('config');
var importerService = require('services/importerService');
var queryService = require('services/queryService');
var csvSerializer = require('serializers/csvSerializer');
var fieldSerializer = require('serializers/fieldSerializer');
var write = require('koa-write');
var passThrough = require('stream').PassThrough;
var redisClient = require('redis').createClient({
    port: config.get('redis.port'),
    host: config.get('redis.host')
});
var fs = require('fs');
var router = new Router({
    prefix: '/csv'
});

var unlink = function(file) {
    return function(callback) {
        fs.unlink(file, callback);
    };
};

var JSONAPIDeserializer = require('jsonapi-serializer').Deserializer;

var deserializer = function(obj) {
    return function(callback) {
        new JSONAPIDeserializer({keyForAttribute: 'camelCase'}).deserialize(obj, callback);
    };
};

var serializeObjToQuery = function(obj){
    return Object.keys(obj).reduce(function(a,k){a.push(k+'='+encodeURIComponent(obj[k]));return a;},[]).join('&');
};


class CSVRouter {

    static *
        import () {
            logger.info('Adding csv with dataset id: ', this.request.body.connector);
            yield importerService.addCSV(this.request.body.connector.connector_url, 'index_' + this.request.body.connector.id.replace(/-/g, ''), this.request.body.connector.id, this.request.body.connector.polygon, this.request.body.connector.point);
            this.body = '';
        }
    static * overwrite () {
            logger.info('Overwrite csv with dataset id: ', this.params.id);
            yield importerService.overwriteCSV(this.request.body.connector.connector_url, 'index_' + this.request.body.connector.id.replace(/-/g, ''), this.request.body.connector.id, this.request.body.connector.polygon, this.request.body.connector.point);
            this.body = '';
        }
    static * query() {
        logger.info('Do Query with dataset', this.request.body);
        this.body = passThrough();   
        const cloneUrl = CSVRouter.getCloneUrl(this.request.url, this.params.dataset);
        logger.debug(this.request.body.dataset);
        yield queryService.doQuery( this.query.sql, this.request.body.dataset.tableName, this.request.body.dataset.id, this.body, cloneUrl);
    }

    static * download() {
        this.body = passThrough();    
        const format = this.query.format ? this.query.format : 'csv';
        this.set('Content-disposition', `attachment; filename=${this.request.body.dataset.id}.${format}`);
        let mimetype;
        switch (format) {
            case 'csv':
                mimetype = 'text/csv';
                break;
            case 'json':
                mimetype = 'application/json';
                break;
        }
        this.set('Content-type', mimetype);
        yield queryService.downloadQuery( this.query.sql, this.request.body.dataset.tableName, this.request.body.dataset.id, this.body, format);
        
        
    }

    static * fields() {
        logger.info('Get fields of dataset', this.request.body);

        let fields = yield queryService.getMapping(this.request.body.dataset.tableName);
        this.body = fieldSerializer.serialize(fields, this.request.body.dataset.tableName, this.request.body.dataset.id);
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
    if(this.statusCode === 200){
        redisClient.setex(this.request.url, config.get('timeCache'), JSON.stringify(this.body));
    }

};

const deserializeDataset = function*(next){
    logger.debug('Body', this.request.body);
    if(this.request.body.dataset && this.request.body.dataset.data){
        this.request.body.dataset = yield deserializer(this.request.body.dataset);
    } else {
        if (this.request.body.dataset && this.request.body.dataset.table_name){
            this.request.body.dataset.tableName = this.request.body.dataset.table_name;
        }
    }
    yield next;
};


const toSQLMiddleware = function*(next) {
    let microserviceClient = require('vizz.microservice-client');
    let options = {
        method: 'GET',
        json: true
    };
    if(!this.query.sql && !this.request.body.sql && !this.query.outFields && !this.query.outStatistics){
        this.throw(400, 'sql or fs required');
        return;
    }

    if (this.query.sql || this.request.body.sql) {
        logger.debug('Checking sql correct');
        let params = Object.assign({}, this.query, this.request.body);
        options.uri = '/convert/sql2SQL?sql=' + params.sql;
        if (params.geostore){
            options.uri += '&geostore=' + params.geostore;
        }
    } else {
        logger.debug('Obtaining sql from featureService');
        let fs = Object.assign({}, this.request.body);
        delete fs.dataset;
        let query = serializeObjToQuery(this.request.query);
        let body = serializeObjToQuery(fs);
        let resultQuery = Object.assign({}, query, body);

        if(resultQuery){
            options.uri = '/convert/fs2SQL' + resultQuery + '&tableName=' + this.request.body.dataset.tableName;
        } else {
            options.uri = '/convert/fs2SQL?tableName=' + this.request.body.dataset.tableName;
        }
    }

    logger.debug(options);
    try {
        let result = yield microserviceClient.requestToMicroservice(options);
        if (result.statusCode === 204 || result.statusCode === 200) {
            this.query.sql = result.body.data.attributes.query;
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

router.post('/query/:dataset', cacheMiddleware, deserializeDataset, toSQLMiddleware, CSVRouter.query);
router.post('/download/:dataset', deserializeDataset, toSQLMiddleware, CSVRouter.download);
router.post('/fields/:dataset', cacheMiddleware, deserializeDataset, CSVRouter.fields);
router.post('/', CSVRouter.import);
router.delete('/:id', CSVRouter.delete);
router.post('/:id/data-overwrite', CSVRouter.overwrite);

module.exports = router;
