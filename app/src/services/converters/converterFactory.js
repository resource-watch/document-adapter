const logger = require('logger');
const CSVConverter = require('services/converters/csvConverter');
const JSONConverter = require('services/converters/jsonConverter');
const XMLConverter = require('services/converters/xmlConverter');
const ConverterNotSupported = require('errors/converterNotSupported');
class Converter {
    static getInstance(type, url, dataPath) {
        logger.info(`Getting converter of type ${type} and dataPath ${dataPath}`);
        switch(type) {
            case 'csv':
                return new CSVConverter(url);
            case 'tsv':
                return new CSVConverter(url, '\t');
            case 'json':
                return new JSONConverter(url, dataPath);
            case 'xml':
                return new XMLConverter(url, dataPath);
            default:
                throw new ConverterNotSupported(400, `Converter to ${type} not supported`);
        }
    }
}

module.exports = Converter;
