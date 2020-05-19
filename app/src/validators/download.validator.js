const logger = require('logger');
const InvalidFormat = require('errors/invalidFormat');

class DownloadValidator {

    static async validateDownload(ctx, next) {
        logger.info('Validating download request');
        ctx.checkQuery('format').optional().isIn(['json', 'csv']);
        if (ctx.errors) {
            logger.info('Error validating dataset creation');
            throw new InvalidFormat(ctx.errors);
        }
        return next();
    }

}

module.exports = DownloadValidator;
