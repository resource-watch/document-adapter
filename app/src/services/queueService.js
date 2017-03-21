'use strict';

const logger = require('logger');
const config = require('config');
const queue = require('bull');
const fs = require('fs');
const co = require('co');
const DownloadService = require('services/downloadService');
const queryService = require('services/queryService');
const ctRegisterMicroservice = require('ct-register-microservice-node');
const ImporterService = require('services/importerService');
const randomstring = require('randomstring');
const ConverterFactory = require('services/converters/converterFactory');

var microserviceClient = null;

const writeFile = function (name, data)  {
    return function (callback) {
        fs.writeFile(name, data, callback);
    };
};

var getKey = function (key) {
    return function (cb) {
        require('redis').createClient({
            port: config.get('redis.port'),
            host: config.get('redis.host')
        }).get(key, cb);
    };
};

class QueueService {
    constructor() {
        logger.info('Creating queue');
        this.importQueue = queue('importer', config.get('redis.port'), config.get('redis.host'));

        this.importQueue.on('on', function (err) {
            logger.error('Error in importqueue', err);
        });
        this.deleteQueue = queue('delete', config.get('redis.port'), config.get('redis.host'));

        this.importQueue.on('error', function (error) {
            logger.error('Error in queue', error);
        });
        this.importQueue.on('failed', function (error) {
            //logger.error('Error in queue', error);
        });
        this.importQueue.on('completed', function (error) {
            logger.info('Completed');
        });
        
        ctRegisterMicroservice.init({
            token: process.env.CT_TOKEN,
            ctUrl: process.env.CT_URL,
            logger
        });
    }

    *
    updateState(id, status, tableName, errorMessage) {
        logger.info('Updating state of dataset ', id, ' with status ', status);

        let options = {
            uri: '/dataset/' + id,
            body: {
               status                
            },
            method: 'PATCH',
            json: true
        };
        if (tableName) {
            options.body.table_name = tableName;
            options.body.tableName = tableName;
        }
        if (errorMessage) {
            options.body.errorMessage = errorMessage;
        }
        logger.info('Updating', options);
        try {
            let result = yield ctRegisterMicroservice.requestToMicroservice(options);
        } catch (e) {
            logger.error(e);
            throw new Error('Error to updating dataset');
        }
    }

    addProcess() {
        //this processes are executed in separated forks
        this.importQueue.process(this.processImport.bind(this));
        this.deleteQueue.process(this.processDelete.bind(this));
    }

    * addDataset(type, url, data, index, id, legend, dataPath) {
        logger.info(`Adding import dataset task with with type ${type}, url ${url}, index ${index}, id ${id} and legend ${legend}`);
        this.importQueue.add({
            url,
            data,
            type,
            dataPath,
            legend: legend,
            index: index,
            id: id,
            action: 'import'
        }, {
            attempts: 2,
            timeout: 86400000, //2 hours
            backoff: 1000
        });
    }

    *
    overwriteDataset(type, url, data, index, id, legend, dataPath) {
        logger.info(`Adding overwrite dataset task with with type ${type}, url ${url}, index ${index}, id ${id} and legend ${legend}`);
        this.importQueue.add({
            url,
            data,
            type,
            dataPath,
            legend: legend,
            index: index,
            id: id,
            action: 'overwrite'
        }, {
            attempts: 2,
            timeout: 86400000, //2 hours
            delay: 1000
        });
    }

    * concatDataset(type, url, data, index, id, legend, dataPath) {
        logger.info(`Adding concat dataset task with with type ${type}, url ${url}, index ${index}, id ${id} and legend ${legend}`);
        this.importQueue.add({
            url,
            data,
            type,
            dataPath,
            legend: legend,
            index: index,
            id: id,
            action: 'concat'
        }, {
            attempts: 2,
            timeout: 86400000, //2 hours
            delay: 1000
        });
    }

    *
    deleteDataset(index, id) {
        logger.info('Adding delete dataset task with id %id and index %s', id, index);
        this.deleteQueue.add({
            index: index,
            id: id
        }, {
            attempts: 3,
            timeout: 86400000, //2 hours
            delay: 1000
        });
    }


    processDelete(job, done) {
        logger.info('Proccesing delete task with index: %s and id: %s', job.data.index, job.data.id);
        co(function* () {
            yield queryService.deleteIndex(job.data.index);
            logger.info('Deleted successfully. Updating state');
        }.bind(this)).then(function () {
            logger.info('Finished deleted task successfully');
            done();
        }, function (error) {
            logger.error(error);
            done(new Error(error));
        });
    }

    processImport(job, done) {
        try {
            logger.info('Proccesing import task with  type: %s, url: %s and index: %s and id: %s and dataPath: %s', job.data.type, job.data.url, job.data.index, job.data.id, job.data.dataPath, job.data.legend);
            co(function* () {
                try {
                    let url = job.data.url;
                    if (job.data.data) {
                        logger.debug('Containg data. Saving in file');
                        url = `/tmp/${randomstring.generate()}.json`;
                        yield writeFile(url, JSON.stringify(job.data.data));
                        job.data.url = url;
                    }

                    logger.debug('Action', job.data.action);
                    yield this.updateState(job.data.id, 0, job.data.index); //pending
                
                    const importer = new ImporterService(job.data.type, job.data.url, job.data.index, job.data.legend, job.data.dataPath, job.data.action);
                    yield importer.startProcess();
                    logger.info('Imported successfully. Updating state');
                    yield this.updateState(job.data.id, 1, job.data.index);
                } catch (err) {
                    logger.error('Error in ProcessImport', err);
                    yield this.updateState(job.data.id, 2, null, err.message || 'Unexpected error');
                    throw err;
                }
            }.bind(this)).then(function () {
                logger.info('Finished imported task successfully');
                done();
            }, function (error) {
                logger.error(error);
                done(new Error(error));
            });
        } catch(err){
            throw err;
        }
    }
}

module.exports = new QueueService();
