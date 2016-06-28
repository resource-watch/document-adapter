'use strict';

var logger = require('logger');
// var JSONAPISerializer = require('jsonapi-serializer').Serializer;
// var csvSerializer = new JSONAPISerializer('csv', {
    // attributes: ['geojson', 'hash', 'providers'],
    // geojson:{
    //     attributes:['type', 'features', 'crs']
    // },
    // providers:{
    //     attributes: ['provider', 'table', 'user']
    // },
    // typeForAttribute: function (attribute, record) {
    //     return attribute;
    // }
// });

class CSVSerializer {

  static serialize(data) {
      logger.debug(data);
      if(data && data.length > 0){
          if(data[0].hits && data[0].hits.hits && data[0].hits.hits.lenght > 0){
              return {
                  data: data[0].hits.hits.map((el) => ({type: 'csvs', attributes:el._source}))
              };
          } else if(data[0].aggregations){
              let keys = Object.keys(data[0].aggregations);
              let attributes = {};
              attributes[keys[0]] = data[0].aggregations[keys[0]].value;
              return {
                  data: [{
                      type: 'csvs',
                      attributes:attributes
                  }]
              };
          }
      }
      return {
          data: []
      };

  }
}

module.exports = CSVSerializer;
