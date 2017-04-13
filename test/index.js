let chai = require('chai');
let chaiAsPromised = require("chai-as-promised");
let faker = require('faker');
chai.use(chaiAsPromised);
let Slave = require('../slave');
let expect = chai.expect;
let assert = chai.assert;

let slave = new Slave({
    host     : 'localhost',
    user     : 'repl',
    password : 'slavepass'
}, {
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows']
});


let knex = require('./knex');
var recentEvent = null;

var lastRow = null;
slave.on('binlog', function(event) {
    recentEvent = event;
    // event.dump();
});

after(function() {
    slave.stop();
});

before(function() {
    return slave.start();
});
beforeEach(function() {
    recentEvent = null;
});

process.on('SIGINT', function() {
    console.log('Got SIGINT.');
    slave.stop();
    process.exit();
});

describe('Replication', function() {
    this.slow(400);
    this.timeout(5000);
    it('Should be able to handle a insert into a table.', function(done) {
        let row = {idtest_table: faker.random.number(), col_1: faker.internet.userName()};
        lastRow = row;
        knex('test_table').insert(row).then(function() {
            setTimeout(function() {
                expect(recentEvent).to.not.be.null;
                expect(recentEvent.rows.length).to.equal(1);
                assert.deepEqual(recentEvent.rows[0], row);
                done();
            }, 30);
        });
    });

    it('Should be able to handle a update a row in a table.', function(done) {
        let newRow = {idtest_table: lastRow.idtest_table, col_1: faker.internet.userName()};
        knex('test_table').where({
            idtest_table: newRow.idtest_table
        }).update(newRow).then(function() {
            let expectation = {
                "before": lastRow,
                "after": newRow
            };

            setTimeout(function() {
                expect(recentEvent).to.not.be.null;
                expect(recentEvent.rows.length).to.equal(1);
                assert.deepEqual(recentEvent.rows[0], expectation);
                done();
            }, 30);
        });
    });
});