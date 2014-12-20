Ext.define('tenrapid.data.proxy.Sqlite', {
	alias: 'proxy.sqlite',
	extend: 'tenrapid.data.proxy.Sql',

	isSQLiteProxy: true,

	config: {
		/**
		 * @cfg {String} filename
		 * File name to access database
		 */
		filename: null,
	},

	executeSql: function(db, sql, values, callback, scope) {
		var args = [sql];
		if (values !== null) {
			args.push(values);
		}
		if (sql.substr(0, 6) === 'SELECT') {
			db.all.apply(db, args.concat(function(error, rows) {
				Ext.callback(callback, scope, [error, {
					rows: rows
				}]);
			}));
		}
		else {
			db.run.apply(db, args.concat(function(error) {
				Ext.callback(callback, scope, [error, {
					insertId: this.lastID,
					rowsAffected: this.changes,
				}]);
			}));
		}
	},

	serializeSqlExecution: function(db, callback, scope) {
		db.serialize(function() {
			Ext.callback(callback, scope, [db]);
		});
	},

	parallelizeSqlExecution: function(db, callback, scope) {
		db.parallelize(function() {
			Ext.callback(callback, scope, [db]);
		});
	},

	getDatabaseObject: function() {
		var filename = this.getFilename();
		if (!filename) {
			Ext.Error.raise('No filename for database given.');
		}
		return this.self.getDatabaseObject(filename);
	},

	closeDatabase: function() {
		var filename = this.getFilename();
		this.self.closeDatabase(filename);
	},

	destroy: function() {
		this.callParent(arguments);
		this.closeDatabase();
	},

	inheritableStatics: {

		sqlite3: null,

		openDatabase: function(filename) {
			return new this.sqlite3.Database(filename);
		},

		closeDatabase: function(filename) {
			var database = this.databaseMap[filename];
			if (database) {
				database.opened--;
				if (database.opened === 0) {
					database.databaseObject.close();
					delete this.databaseMap[filename];
				}
			}
		},

	}
},
function() {
	if (window.require) {
		this.sqlite3 = require('sqlite3');//.verbose();
	}
});