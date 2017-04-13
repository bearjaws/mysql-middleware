function getTableSchema(database, table) {
    let queryString = 'SELECT ' +
        'COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, ' +
        'COLUMN_COMMENT, COLUMN_TYPE ' +
        "FROM columns " + "WHERE table_schema='{{database}}' AND table_name='{{table}}'";
    return queryString.replace('{{database}}', database).replace('{{table}}', table);
}

function getChecksum() {
    return 'select @@GLOBAL.binlog_checksum as checksum';
}
module.exports = {
    getChecksum: getChecksum,
    getTableSchema: getTableSchema
};