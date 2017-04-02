// Client code
var Slave = require('./slave');

var slave = new Slave({
    host     : 'localhost',
    user     : 'repl',
    password : 'slavepass'
});

slave.on('binlog', function(evt) {
    evt.dump();
});

slave.start({
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows']
});

process.on('SIGINT', function() {
    console.log('Got SIGINT.');
    slave.stop();
    process.exit();
});
