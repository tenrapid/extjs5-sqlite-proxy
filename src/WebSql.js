Ext.define('tenrapid.data.proxy.WebSql', {
	alias: 'proxy.websql',
	extend: 'tenrapid.data.proxy.Sql',

	isWebSqlProxy: true,

	config: {
		/**
		 * @cfg {String} database
		 * Name of database to access
		 */
		database: null,
	},

	executeSql: function(tx, sql, values, callback, scope) {
		tx.executeSql(sql, values,
			function(tx, sqlResultSet) {
				var sqlResultSetRowList = sqlResultSet.rows,
					sqlResultSetRowListLen = sqlResultSetRowList.length,
					resultSet = {
						rowsAffected: sqlResultSet.rowsAffected
					},
					rows,
					i;

				if (sqlResultSetRowListLen) {
					rows = [];
					for (i = 0; i < sqlResultSetRowListLen; i++) {
						// clone the row items because its properties are readonly and field converters
						// may want to write to them
						rows.push(Ext.apply({}, sqlResultSetRowList.item(i)));
					}
					resultSet.rows = rows;
				}
				if (sql.substr(0, 6) == 'INSERT') {
					resultSet.insertId = sqlResultSet.insertId
				}
				Ext.callback(callback, scope, [null, resultSet]);
			},
			function(tx, error) {
				Ext.callback(callback, scope, [error]);
			}
		);
	},

	serializeSqlExecution: function(db, callback, scope) {
		db.transaction(function(tx) {
			Ext.callback(callback, scope, [tx]);
		});
	},

	parallelizeSqlExecution: function(tx, callback, scope) {
		Ext.callback(callback, scope, [tx]);
	},

	getDatabaseObject: function() {
		var database = this.getDatabase();
		if (!database) {
			Ext.Error.raise('No database name given.');
		}
		return this.self.getDatabaseObject(database);
	},

	inheritableStatics: {

		openDatabase: function(database) {
			return openDatabase(database, '1.0', 'Sencha Database', 5 * 1024 * 1024);
		},

	},
});