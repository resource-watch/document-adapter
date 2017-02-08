'use strict';

var Router = require('koa-router');
var logger = require('logger');
var config = require('config');
var importerService = require('services/importerService');
var queryService = require('services/queryService');
var csvSerializer = require('serializers/csvSerializer');
var fieldSerializer = require('serializers/fieldSerializer');
var write = require('koa-write');
const Json2sql = require('sql2json').json2sql;
var passThrough = require('stream').PassThrough;
var redisClient = require('redis').createClient({
    port: config.get('redis.port'),
    host: config.get('redis.host')
});
var IndexNotFound = require('errors/indexNotFound');

const ctRegisterMicroservice = require('ct-register-microservice-node');
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

    static * import () {
        logger.info('Adding csv with dataset id: ', this.request.body);
        yield importerService.addCSV(this.request.body.connectorUrl, this.request.body.dataset.tableName, this.request.body.dataset.id, this.request.body.dataset.legend);
        this.body = '';
    }

    static * overwrite () {
        logger.info('Overwrite csv with dataset id: ', this.params.id);
        yield importerService.overwriteCSV(this.request.body.connector.connector_url, this.request.body.connector.table_name, this.request.body.connector.id, this.request.body.connector.legend);
        this.body = '';
    }

    static * concat () {
        logger.info('Concat csv with dataset id: ', this.params.dataset);
        this.assert(this.request.body.url, 400, 'Url is required');
        yield importerService.concatCSV(this.request.body.url, this.request.body.dataset.tableName, this.request.body.dataset.id, this.request.body.dataset.legend);
        this.body = '';
    }
    
    static * query() {
        logger.info('Do Query with dataset', this.request.body);
        logger.debug('Checking if is delete or select');

        try {
            if (this.state.parsed.delete) {
                logger.debug('Doing delete');
                this.state.parsed.from = this.request.body.dataset.tableName;
                const sql = Json2sql.toSQL(this.state.parsed);
                this.body = yield queryService.doDeleteQuery(sql, this.state.parsed, this.request.body.dataset.tableName);
            } else  if (this.state.parsed.select) {
                this.body = passThrough();
                const cloneUrl = CSVRouter.getCloneUrl(this.request.url, this.params.dataset);
                logger.debug(this.request.body.dataset);
                yield queryService.doQuery( this.query.sql, this.state.parsed, this.request.body.dataset.tableName, this.request.body.dataset.id, this.body, cloneUrl);
            } else {
                this.throw(400, 'Query not valid');
                return;
            }
        } catch(err) {
            logger.error(err);
            throw err;            
        }
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
        yield queryService.downloadQuery( this.query.sql, this.state.parsed, this.request.body.dataset.tableName, this.request.body.dataset.id, this.body, format);


    }

    static * fields() {
        logger.info('Get fields of dataset', this.request.body);

        let fields = yield queryService.getMapping(this.request.body.dataset.tableName);
        this.body = fieldSerializer.serialize(fields, this.request.body.dataset.tableName, this.request.body.dataset.id);
    }

    static getCloneUrl(url, idDataset) {
        return {
            http_method: 'POST',
            url: `/dataset/${idDataset}/clone`,
            body: {
                dataset: {
                    datasetUrl: url.replace('/csv', ''),
                    application: ['your','apps']
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
        let result = yield ctRegisterMicroservice.requestToMicroservice(options);
        this.query.sql = result.data.attributes.query;
        this.state.parsed = result.data.attributes.jsonSql;
        logger.debug(this.query.sql);
        yield next;
        
    } catch (e) {
        if(e.statusCode === 400){
            this.status = e.statusCode;
            this.body = e.body;
        }
        throw e;
    }
};


const containApps = function(apps1, apps2) {
    if (!apps1 || !apps2){
        return false;
    }
    for (let i = 0, length = apps1.length; i < length; i++) {
        for (let j = 0, length2 = apps2.length; j < length2; j++){
            if (apps1[i] === apps2[j]){
                return true;
            }
        }
    }
    return false;
};

const checkUserHasPermission = function(user, dataset) {
    if (user && dataset) {        
         // check if user is admin of any application of the dataset or manager and owner of the dataset
        if (user.role === 'MANAGER' && user.id === dataset.userId){
            return true;
        } else  if (user.role === 'ADMIN' && containApps(dataset.application, user.extraUserData ? user.extraUserData.apps : null)) {
            return true;
        }
        
    }
    return false;
};

const checkPermissionDelete = function *(next) {
    if (this.state.parsed.delete) {
        if (this.request.body && this.request.body.loggedUser) {

            if (checkUserHasPermission(this.request.body.loggedUser, this.request.body.dataset)){
                yield next;
                return;
            } else {
                this.throw(403, 'Not authorized to execute DELETE query');
                return;
            }
        }        
    } 
    yield next;
};

const checkPermissionModify = function *(next){
    logger.debug('Checking if the user has permissions');
    const user = this.request.body.loggedUser;
    const dataset = this.request.body.dataset;
    if (checkUserHasPermission(user, dataset)){
        if (dataset.overwrite) {
            yield next;
            return;
        }
        this.throw(409, 'Dataset locked. Overwrite false.');
        return;
        
    } else {
        this.throw(403, 'Not authorized');
        return;
    }
};

router.post('/query/:dataset', cacheMiddleware, deserializeDataset, toSQLMiddleware, checkPermissionDelete, CSVRouter.query);
router.post('/download/:dataset', deserializeDataset, toSQLMiddleware, CSVRouter.download);
router.post('/fields/:dataset', cacheMiddleware, deserializeDataset, CSVRouter.fields);
router.post('/', CSVRouter.import);
router.delete('/:id', CSVRouter.delete);
router.post('/:id/data-overwrite', deserializeDataset, checkPermissionModify, CSVRouter.overwrite);
router.post('/concat/:dataset', deserializeDataset, checkPermissionModify, CSVRouter.concat);

module.exports = router;
