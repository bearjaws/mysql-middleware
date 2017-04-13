let _ = require('lodash');
let bluebird = require('bluebird');
let EventEmitter = require('events').EventEmitter;
let mysql = require('mysql');
let util = require('util');

bluebird.promisifyAll(require("mysql/lib/Connection").prototype);
bluebird.promisifyAll(require("mysql/lib/Pool").prototype);

let generateBinlog = require('./lib/sequence/binlog');
let queries = require('./lib/queries');
function Slave(databaseConfig, options) {
  this.set(options);

  EventEmitter.call(this);

  // to send table info query
  var clonedConfig = _.cloneDeep(databaseConfig);
  clonedConfig.database = 'information_schema';
  this.ctrlConnection = mysql.createConnection(clonedConfig);
  this.ctrlConnection.on('error', this._emitError.bind(this));
  this.ctrlConnection.on('unhandledError', this._emitError.bind(this));

  this.ctrlConnection.connect();
  this.ctrlCallbacks = [];

  this.connection = mysql.createConnection(databaseConfig);
  this.connection.on('error', this._emitError.bind(this));
  this.connection.on('unhandledError', this._emitError.bind(this));

  this.tableMap = {};
  this.ready = false;
  this.useChecksum = false;
  // Include 'rotate' events to keep these properties updated
  this.binlogName = null;
  this.binlogNextPos = null;
    this.binlogOptions = {
        tableMap: this.tableMap,
    };
}

util.inherits(Slave, EventEmitter);

Slave.prototype.start = function() {
  var self = this;
  this.binlogOptions = {
    tableMap: self.tableMap,
  };

    return this._isChecksumEnabled().then(function() {
        return self._findBinlogEnd();
    }).then(function() {
        return self.slaveReady();
    }).then(function() {
        return self.startSlave(self.options);
    });
};

Slave.prototype.slaveReady = function() {
    let self = this;
    // Run asynchronously from _init(), as serverId option set in start()
    if(self.options.serverId !== undefined){
        self.binlogOptions.serverId = self.options.serverId;
    }

    if(('binlogName' in self.options) && ('binlogNextPos' in self.options)) {
        self.binlogOptions.filename = self.options.binlogName;
        self.binlogOptions.position = self.options.binlogNextPos
    }

    self.binlog = generateBinlog.call(self, self.binlogOptions);
    self.ready = true;
};

Slave.prototype._isChecksumEnabled = function() {
  var self = this;
  var ctrlConnection = self.ctrlConnection;
  var connection = self.connection;

  return ctrlConnection.queryAsync(queries.getChecksum()).then(function(rows){
      var checksumEnabled = true;
      if (rows[0].checksum === 'NONE') {
          checksumEnabled = false;
      }

      var setChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';
      if (checksumEnabled === true) {
          return connection.queryAsync(setChecksumSql).catch(function () {
              self.emit('error', err);
          }).finally(function() {
              self.useChecksum = checksumEnabled;
              self.binlogOptions.useChecksum = checksumEnabled;
              return bluebird.resolve();
          });
      }
  }).catch(function(err) {
      if(err.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)) {
          return false;
      }
      self.emit('error', err);
      return false;
  });
};

Slave.prototype._findBinlogEnd = function() {
  var self = this;
  return self.ctrlConnection.queryAsync('SHOW BINARY LOGS').then(function(rows) {
      let result = rows.length > 0 ? rows[rows.length - 1] : null;
      if(result && self.options.startAtEnd){
          self.binlogOptions.filename = result.Log_name;
          self.binlogOptions.position = result.File_size;
      }
  }).catch(function(err) {
      self.emit('error', err);
  });
};

Slave.prototype._fetchTableInfo = function(tableMapEvent) {
  var self = this;
  var sql = queries.getTableSchema(tableMapEvent.schemaName, tableMapEvent.tableName);

  return this.ctrlConnection.queryAsync(sql).then(function(rows) {
      if (rows.length === 0) {
          self.emit('error', new Error(
              'Insufficient permissions to access: ' +
              tableMapEvent.schemaName + '.' + tableMapEvent.tableName));
          return;
      }
      self.tableMap[tableMapEvent.tableId] = {
          columnSchemas: rows,
          parentSchema: tableMapEvent.schemaName,
          tableName: tableMapEvent.tableName
      };
  }).catch(function(err) {
      // No way to recover.
      self.emit('error', err);
      return;
  });
};

Slave.prototype.set = function(options){
  this.options = options || {};
};

Slave.prototype.startSlave = function(options) {
  var self = this;
  self.set(options);

    self.connection._implyConnect();
    self.connection._protocol._enqueue(new self.binlog(function(error, event){
      if(error) return self.emit('error', error);
      // Do not emit events that have been filtered out
      if(event === undefined || event._filtered === true) return;

      switch(event.getTypeName()) {
        case 'TableMap':
          var tableMap = self.tableMap[event.tableId];

          if (!tableMap) {
            self.connection.pause();

            return self._fetchTableInfo(event).then(function() {
                event.updateColumnInfo();
                self.emit('binlog', event);
                self.connection.resume();
            });
          }
          break;
        case 'Rotate':
          if (self.binlogName !== event.binlogName) {
            self.binlogName = event.binlogName;
          }
          break;
      }
      self.binlogNextPos = event.nextPosition;
      self.emit('binlog', event);
    }));

    return bluebird.resolve();
};

Slave.prototype.stop = function(){
  var self = this;
  // Binary log connection does not end with destroy()
  self.connection.destroy();
  self.ctrlConnection.query(
    'KILL ' + self.connection.threadId,
    function(error, reuslts){
      self.ctrlConnection.destroy();
    }
  );
};

Slave.prototype._skipEvent = function(eventName){
  var include = this.options.includeEvents;
  var exclude = this.options.excludeEvents;
  return !(
   (include === undefined ||
    (include instanceof Array && include.indexOf(eventName) !== -1)) &&
   (exclude === undefined ||
    (exclude instanceof Array && exclude.indexOf(eventName) === -1)));
};

Slave.prototype._skipSchema = function(database, table){
  var include = this.options.includeSchema;
  var exclude = this.options.excludeSchema;
  return !(
   (include === undefined ||
    (database !== undefined && (database in include) &&
     (include[database] === true ||
      (include[database] instanceof Array &&
       include[database].indexOf(table) !== -1)))) &&
   (exclude === undefined ||
      (database !== undefined &&
       (!(database in exclude) ||
        (exclude[database] !== true &&
          (exclude[database] instanceof Array &&
           exclude[database].indexOf(table) === -1))))));
};

Slave.prototype._emitError = function(error) {
  this.emit('error', error);
};

module.exports = Slave;
