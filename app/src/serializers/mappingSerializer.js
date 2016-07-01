'use strict';

var logger = require('logger');

class CSVSerializer {

    static searchProperties(el){
        if(!el.properties && Object.keys(el).length === 1){
            return CSVSerializer.searchProperties(el[Object.keys(el)[0]]);
        }
        return el.properties;
    }

    static serialize(data) {
        if(data && data.length > 0){
            return CSVSerializer.searchProperties(data[0]);
        }
    }
}

module.exports = CSVSerializer;
