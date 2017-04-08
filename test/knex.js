let knex = require('knex')({
    client: 'mysql',
    connection: {
        host : '127.0.0.1',
        user : 'middleware',
        password : 'middleware',
        database : 'middleware'
    }
});


module.exports = knex;