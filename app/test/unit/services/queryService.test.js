'use strict';
var logger = require('logger');
var should = require('should');
var assert = require('assert');
var QueryService = require('services/queryService');

describe('Generate correct SQL', function() {


    before(function*() {

    });

    it('Without select', function() {
        let data = {
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT * FROM myTable';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('Only with select', function() {
        let data = {
            select: ['name', 'surname'],
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With order asc', function() {
        let data = {
            select: ['name', 'surname'],
            order: ['name'],
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable ORDER BY name ASC';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With order desc', function() {
        let data = {
            select: ['name', 'surname'],
            order: ['-name'],
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable ORDER BY name DESC';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With order asc and desc', function() {
        let data = {
            select: ['name', 'surname'],
            order: ['-name', 'surname'],
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable ORDER BY name DESC, surname ASC';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With aggrBy', function() {
        let data = {
            select: ['name', 'surname'],
            aggrBy: ['name', 'surname'],
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable GROUP BY name, surname';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With limit', function() {
        let data = {
            select: ['name', 'surname'],
            limit: 1,
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable LIMIT 1';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With filter IN', function() {
        let data = {
            select: ['name', 'surname'],
            filter: 'id==1,2,4,5',
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable WHERE id IN (1,2,4,5)';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With filter ===', function() {
        let data = {
            select: ['name', 'surname'],
            filter: 'id==1',
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable WHERE id = 1';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With filter BETWEEN', function() {
        let data = {
            select: ['name', 'surname'],
            filter: 'value><350558..9506590',
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable WHERE value BETWEEN 350558 AND 9506590';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With several filters', function() {
        let data = {
            select: ['name', 'surname'],
            filter: 'id==1,2,4,5 <and> value><350558..9506590',
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable WHERE id IN (1,2,4,5) AND value BETWEEN 350558 AND 9506590';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });

    it('With several filters NOT', function() {
        let data = {
            select: ['name', 'surname'],
            filterNot: 'id==1,2,4,5 <and> value><350558..9506590',
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT name, surname FROM myTable WHERE NOT (id IN (1,2,4,5) AND value BETWEEN 350558 AND 9506590)';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });


    it('With aggrBy and aggrColumns', function() {
        let data = {
            select: ['state'],
            aggrBy: ['country', 'state'],
            aggrColumns: ['sum(age)', 'avg(age)'],
            dataset:{
                index: 'myTable'
            }
        };
        let sqlResult = 'SELECT state, sum(age), avg(age) FROM myTable GROUP BY country, state';

        let query = QueryService.convertToSQL(data.select, data.order, data.aggrBy, data.filter, data.filterNot, data.limit, data.aggrColumns, data.dataset.index);

        query.should.be.a.String();
        query.should.be.equal(sqlResult);
    });


    after(function*() {

    });
});
