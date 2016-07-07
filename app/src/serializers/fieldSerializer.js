'use strict';

var logger = require('logger');

class FieldSerializer {

    static searchProperties(el){
        if(!el.properties && Object.keys(el).length === 1){
            return FieldSerializer.searchProperties(el[Object.keys(el)[0]]);
        }
        return el.properties;
    }

    static serialize(data) {
        if(data && data.length > 0){
            return FieldSerializer.searchProperties(data[0]);
        }
    }
}

module.exports = FieldSerializer;
