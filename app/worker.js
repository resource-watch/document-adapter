'use strict';

const queueService = require('services/queueService');
const logger = require('logger');
queueService.addProcess();
logger.info('Worker started');
