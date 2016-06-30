'use strict';

const importerService = require('services/importerService');
const logger = require('logger');
importerService.addProcess();
logger.info('Worker started');
