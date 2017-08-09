'use strict';

var Router = require('koa-router');
var logger = require('logger');
var config = require('config');
var queueService = require('services/queueService');
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
    prefix: '/document'
});

var unlink = function (file) {
    return function (callback) {
        fs.unlink(file, callback);
    };
};

var JSONAPIDeserializer = require('jsonapi-serializer').Deserializer;

var deserializer = function (obj) {
    return function (callback) {
        new JSONAPIDeserializer({
            keyForAttribute: 'camelCase'
        }).deserialize(obj, callback);
    };
};

var serializeObjToQuery = function (obj) {
    return Object.keys(obj).reduce(function (a, k) {
        a.push(k + '=' + encodeURIComponent(obj[k]));
        return a;
    }, []).join('&');
};


class QueryRouter {


    static * query() {
        logger.info('Do Query with dataset', this.request.body);
        logger.debug('Checking if is delete or select');

        try  {
            if (this.state.parsed.delete) {
                logger.debug('Doing delete');
                this.state.parsed.from = this.request.body.dataset.tableName;
                const sql = Json2sql.toSQL(this.state.parsed);
                this.body = yield queryService.doDeleteQuery(sql, this.state.parsed, this.request.body.dataset.tableName);
            } else if (this.state.parsed.select) {
                this.body = passThrough();
                const cloneUrl = QueryRouter.getCloneUrl(this.request.url, this.params.dataset);
                this.state.parsed.from = this.request.body.dataset.tableName;
                const sql = Json2sql.toSQL(this.state.parsed);
                logger.debug(this.request.body.dataset);
                yield queryService.doQuery(sql, this.state.parsed, this.request.body.dataset.tableName, this.request.body.dataset.id, this.body, cloneUrl, this.query.format);
            } else {
                this.throw(400, 'Query not valid');
                return;
            }
        } catch (err) {
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
            case 'geojson':
                mimetype = 'application/json';
                break;
        }
        this.set('Content-type', mimetype);
        this.state.parsed.from = this.request.body.dataset.tableName;
        const sql = Json2sql.toSQL(this.state.parsed);
        yield queryService.downloadQuery(sql, this.state.parsed, this.request.body.dataset.tableName, this.request.body.dataset.id, this.body, format);


    }

    static * fields() {
        logger.info('Get fields of dataset', this.request.body);

        let fields = yield queryService.getMapping(this.request.body.dataset.tableName);
        this.body = fieldSerializer.serialize(fields, this.request.body.dataset.tableName, this.request.body.dataset.id);
    }

    static getCloneUrl(url, idDataset) {
        return {
            http_method: 'POST',
            url: `/${process.env.API_VERSION}/dataset/${idDataset}/clone`,
            body: {
                dataset: {
                    datasetUrl: `/${process.env.API_VERSION}${url.replace('/document', '')}`,
                    application: ['your', 'apps']
                }
            }
        };
    }

}

const deserializeDataset = function* (next) {
    logger.debug('Body', this.request.body);
    if (this.request.body.dataset && this.request.body.dataset.data) {
        this.request.body.dataset = yield deserializer(this.request.body.dataset);
    } else {
        if (this.request.body.dataset && this.request.body.dataset.table_name) {
            this.request.body.dataset.tableName = this.request.body.dataset.table_name;
        }
    }
    yield next;
};


const toSQLMiddleware = function* (next) {

    let options = {
        method: 'GET',
        json: true
    };
    if (!this.query.sql && !this.request.body.sql && !this.query.outFields && !this.query.outStatistics) {
        this.throw(400, 'sql or fs required');
        return;
    }

    if (this.query.sql || this.request.body.sql) {
        logger.debug('Checking sql correct');
        let params = Object.assign({}, this.query, this.request.body);
        options.uri = '/convert/sql2SQL?sql=' + params.sql;
        if (params.geostore) {
            options.uri += '&geostore=' + params.geostore;
        }
        if (params.geojson) {
            options.body = {
                geojson: params.geojson
            };
            options.method = 'POST';
        }
    } else {
        logger.debug('Obtaining sql from featureService');
        let fs = Object.assign({}, this.request.body);
        delete fs.dataset;
        let query = serializeObjToQuery(this.request.query);
        let body = fs;
        let resultQuery = Object.assign({}, query);

        if (resultQuery) {
            options.uri = '/convert/fs2SQL' + resultQuery + '&tableName=' + this.request.body.dataset.tableName;
        } else {
            options.uri = '/convert/fs2SQL?tableName=' + this.request.body.dataset.tableName;
        }
        options.body = body;
        options.method = 'POST';
    }

    logger.debug(options);
    try {
        let result = yield ctRegisterMicroservice.requestToMicroservice(options);
        this.query.sql = result.data.attributes.query;
        this.state.parsed = result.data.attributes.jsonSql;
        logger.debug(this.query.sql);
        yield next;

    } catch (e) {
        if (e.statusCode === 400 || e.statusCode === 404) {
            this.status = e.statusCode;
            this.body = e.body;
        }
        throw e;
    }
};


const containApps = function (apps1, apps2) {
    if (!apps1 || !apps2) {
        return false;
    }
    for (let i = 0, length = apps1.length; i < length; i++)  {
        for (let j = 0, length2 = apps2.length; j < length2; j++) {
            if (apps1[i] === apps2[j]) {
                return true;
            }
        }
    }
    return false;
};

const checkUserHasPermission = function (user, dataset) {

    if (user && dataset) {
        if (user.id === 'microservice') {
            return true;
        }
        // check if user is admin of any application of the dataset or manager and owner of the dataset
        if (user.role === 'MANAGER' && user.id === dataset.userId) {
            return true;
        } else if (user.role === 'ADMIN' && containApps(dataset.application, user.extraUserData ? user.extraUserData.apps : null)) {
            return true;
        }

    }
    return false;
};

const checkPermissionDelete = function* (next) {
    if (this.state.parsed.delete) {
        if (this.request.body && this.request.body.loggedUser) {

            if (checkUserHasPermission(this.request.body.loggedUser, this.request.body.dataset)) {
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

const cacheMiddleware = function* (next) {
    let url = '';
    if (this.request && this.request.body ) {
        if (this.request.body.sql){
            url = `/document/query/${this.params.dataset}?sql=${this.request.body.sql}`;
        } else if (this.request.query.sql) {
            url = `/document/query/${this.params.dataset}?sql=${this.request.query.sql}`;
        } else if (this.request.body.fs) {
            url = `/document/query/${this.params.dataset}?fs=${JSON.stringify(this.request.body.fs)}`;
        }
        if (this.request.body.geostore) {
            url += `&geostore=${this.request.body.geostore}`;
        }
    } else {
        url = this.request.url;
    }
    const data = yield redisClient.getAsync(`${url}-data`);
    logger.info('Entering in cache', `${url}-data`);

    if (data && this.headers['cache-control'] !== 'no-cache') {
        logger.info('Exist data in cache');
        let headers = yield redisClient.getAsync(`${url}-headers`);
        if (headers) {
            headers = JSON.parse(headers);
        }
        try {
            this.body = JSON.parse(data);
            const keys = Object.keys(headers);
            for (let i = 0, length = keys.length; i < length; i++) {
                this.set(keys[i], headers[keys[i]]);
            }
            return;
        } catch (e) {
            logger.error(e);
            this.body = null;
        }
    }

    yield next;

    if (this.body.on) {
        this.body.on('data', (chunk) => {
            logger.debug('Seting response', `${url}-data`);
            redisClient.append(`${url}-data`, chunk);
        });

        this.body.on('end', () => {
            logger.debug('Seting responseend');
            if (this.res.statusCode >= 200 && this.res.statusCode < 300) {
                logger.debug('Saving headers');
                redisClient.set(`${url}-headers`, JSON.stringify(this.headers));
            } else {
                logger.debug('Removing key by error');
                redisClient.del(`${url}-data`);
            }
        });

    } else {

        redisClient.set(`${url}-data`, JSON.stringify(this.body));
        redisClient.set(`${url}-headers`, JSON.stringify(this.headers));
    }
};



router.post('/query/:dataset', cacheMiddleware, deserializeDataset, toSQLMiddleware, checkPermissionDelete, QueryRouter.query);
router.post('/download/:dataset', deserializeDataset, toSQLMiddleware, QueryRouter.download);
router.post('/fields/:dataset', cacheMiddleware, deserializeDataset, QueryRouter.fields);



module.exports = router;
