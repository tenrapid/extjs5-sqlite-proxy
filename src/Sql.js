Ext.define('tenrapid.data.proxy.Sql', {
	alias: 'proxy.sql',
	extend: 'Ext.data.proxy.Client',
	
	requires: [
		'Ext.data.Request',
	],

	isSqlProxy: true,

	config: {
		/**
		 * @cfg {String} table
		 * Optional Table name to use if not provided model.entityName will be used
		 */
		table: null,

		idParam: 'id',

		uniqueIdStrategy: false,

		debug: false
	},

	isSynchronous: false,

	isHeterogeneousTreeStoreProxy: false,

	tables: null,

	constructor: function(config) {
		this.tables = {};
		this.callParent(arguments);

		//<debug>
		this.on('exception', function(proxy, operation) {
			var errors = Ext.Array.from(operation.getError());
			console.error('[E]\t Proxy exception:', operation.getError());
			Ext.each(errors, function(error) {
				console.log('\t', error.error ? error.error.message : error.message, error);
			});
		});
		//</debug>
	},

	log: function() {
		//<debug>
		if (this.debug) {
			console.log.apply(console, arguments);
		}
		//</debug>
	},

	updateModel: function(model, oldModel) {
		if (model) {
			var proto = model.prototype;
			this.isHeterogeneousTreeStoreProxy = proto.isNode && !!proto.childType;
		}
		if (oldModel) {
			delete this.tables[oldModel.entityName];
		}
		this.callParent(arguments);
	},

	applyTable: function(table) {
		var model = this.getModel(),
			name;
		if (Ext.isString(table)) {
			name = table;
			table = this.getTableInfo(model);
			table.name = name;
		}
		return table;
	},

	updateTable: function(table) {
		var model = this.getModel();
		this.tables[model.entityName] = table;
	},

	getTable: function(model) {
		if (!model) {
			model = this.getModel();
		}
		if (!this.tables[model.entityName]) {
			this.tables[model.entityName] = this.getTableInfo(model);
		}
		return this.tables[model.entityName];
	},

	create: function (operation) {
		this.doRequest(operation);
	},

	read: function (operation) {
		this.doRequest(operation);
	},

	update: function (operation) {
		this.doRequest(operation);
	},

	erase: function (operation) {
		this.doRequest(operation);
	},

	doRequest: function(operation) {
		this.log('"' + operation.action + '" operation', operation);
		var request = this.buildRequest(operation);

		this.queryDatabase(request.getCurrentConfig());
	},

	buildRequest: function(operation) {
		var me = this,
			params = operation.getParams() || {},
			writer = me.getWriter(),
			request,
			operationId,
			idParam,
			table;

		operationId = operation.getId();
		idParam = me.getIdParam();
		if (operationId !== undefined && params[idParam] === undefined) {
			params[idParam] = operationId;
		}

		table = me.getTable();

		if (operation.isReadOperation) {
			var node = operation.getInitialConfig().node,
				childType;

			if (node && node.isNode) {
				params.parentIdProperty = node.getTreeStore().getParentIdProperty() || 'parentId';

				if (childType = node.childType) {
					table = me.getTable(node.schema.getEntity(childType));
				}
			}

			params = Ext.apply(params, {
				page: operation.getPage(),
				start: operation.getStart(),
				limit: operation.getLimit(),
				sorters: operation.getSorters(),
				filters: operation.getFilters(),
			});
		}

		request = Ext.create('Ext.data.Request', {
			params: params,
			action: operation.getAction(),
			records: operation.getRecords(),
			operation: operation,
		});

		if (writer && operation.allowWrite()) {
			request = writer.write(request);
		}

		request.setConfig({
			callback: function(error, rows) {
				me.processRows(error, operation, request, rows);
			},
			scope: me,
		});
		// param.table is set here, so it is not cloned by request.getCurrentConfig() in
		// doRequest()
		request.setParam('table', table);

		operation.setRequest(request);

		return request;
	},

	processRows: function(error, operation, request, rows) {
		var me = this,
			node = operation.getInitialConfig().node,
			reader = me.getReader(),
			exception,
			resultSet;

        me.fireEvent('beginprocessresponse', me, rows, operation);
		if (error) {
			operation.setException(error);
			exception = true;
		}
		else {
			if (node && node.childType) {
				reader = node.schema.getEntity(node.childType).getProxy().getReader();
			}
			resultSet = reader.read(rows, {
				recordCreator: operation.getRecordCreator()
			});
			operation.process(resultSet);
			exception = !operation.wasSuccessful();
		}
		if (exception) {
			me.fireEvent('exception', me, operation);
		}
        me.fireEvent('endprocessresponse', me, rows, operation);
	},

	createTableIfNotExists: function (db, table, callback, scope) {
		var me = this;
		if (!table.exists) {
			me.executeSql(db, 'CREATE TABLE IF NOT EXISTS ' + table.name + ' (' + table.schemaString + ')', null, function(error) {
				me.log('CREATE TABLE IF NOT EXISTS ' + table.name + ' (' + table.schemaString + ')');
				if (error === null) {
					table.exists = true;
				}
				Ext.callback(callback, scope, [error]);
			});
		}
		else {
			Ext.callback(callback, scope, [null]);
		}
	},

	queryDatabase: function(request) {
		var me = this,
			db = me.getDatabaseObject(),
			isSelect = request.operation.isReadOperation,
			tables = isSelect || !me.isHeterogeneousTreeStoreProxy ?
				[request.params.table] :
				Ext.Array.unique(Ext.Array.map(request.records, me.getTable, me)),
			tablesToCreate = Ext.Array.filter(tables, function(table) {
				return !table.exists;
			}),
			barrier;

		function query(tx) {
			if (isSelect) {
				me.selectRecords(tx, request);
			}
			else {
				me.writeRecords(tx, request);
			}
		}

		me.serializeSqlExecution(db, function(tx) {
			if (tablesToCreate.length) {
				barrier = Ext.Function.createBarrier(tablesToCreate.length, query.bind(me, tx));
				Ext.each(tablesToCreate, function(table) {
					me.createTableIfNotExists(tx, table, barrier);
				});
			}
			else {
				query(tx);
			}
		});
	},

	selectRecords: function(db, request) {
		var me = this,
			params = request.params,
			table = params.table,
			parentIdProperty = params.parentIdProperty,
			id = params[me.getIdParam()],
			sql = 'SELECT * FROM ' + table.name,
			filterStatement = ' WHERE ',
			sortStatement = ' ORDER BY ',
			i, ln, filter, sorter, property, value;

		if (id !== undefined && parentIdProperty === undefined) {
			sql += filterStatement + table.idProperty + ' = ' + id;
		}
		else {
			// handle start, limit, sort, filter and group params
			ln = params.filters && params.filters.length;
			if (ln) {
				for (i = 0; i < ln; i++) {
					filter = params.filters[i];
					property = table.columns[filter.getProperty()];
					value = filter.getValue();
					if (property !== null) {
						sql += filterStatement + property + ' ' + (filter.getAnyMatch() ? ('LIKE \'%' + value + '%\'') : ('= \'' + value + '\''));
						filterStatement = ' AND ';
					}
				}
			}
			if (parentIdProperty !== undefined) {
				// TreeStore: get child nodes
				sql += filterStatement + parentIdProperty + ' = \'' + id + '\'';
			}

			ln = params.sorters && params.sorters.length;
			if (ln) {
				for (i = 0; i < ln; i++) {
					sorter = params.sorters[i];
					property = table.columns[sorter.getProperty()];
					if (property !== null) {
						sql += sortStatement + property + ' ' + sorter.getDirection();
						sortStatement = ', ';
					}
				}
			}

			if (params.page !== undefined) {
				sql += ' LIMIT ' + parseInt(params.start, 10) + ', ' + parseInt(params.limit, 10);
			}
		}

		me.executeSql(db, sql, null, function(error, resultSet) {
			me.log(sql, '\n\trequest:', request, '\n\trows:', resultSet.rows);
			Ext.callback(request.callback, request.scope || me, [error, resultSet.rows]);
		});
	},

	writeRecords: function(db, request) {
		var me = this,
			action = request.action,
			queryBuilder = Ext.Function.alias(me, 'build' + Ext.String.capitalize(action) + 'Query'),
			records = request.records,
			writtenData = Ext.Array.from(request.jsonData),
			totalRecords = records.length,
			executed = 0,
			errors = [],
			rows = [];

		me.parallelizeSqlExecution(db, function(db) {
			Ext.each(records, function(record, i) {
				var id = record.getId(),
					data = writtenData[i],
					table = me.getTable(record),
					query = queryBuilder(table, record, data),
					row;

				me.executeSql(db, query.sql, query.values, function(error, resultSet) {
					me.log(query.sql, query.values, error || '');
					if (error) {
						errors.push({
							id: id,
							error: error
						});
					}
					else {
						row = {};
						row[table.clientIdProperty] = id;
						if (action === 'create' && !table.uniqueIdStrategy) {
							row[table.idProperty] = resultSet.insertId;
						}
						rows.push(row);
					}
					executed++;
					if (executed === totalRecords) {
						Ext.callback(request.callback, request.scope || me, [errors.length > 0 ? errors : null, rows]);
					}
				});
			});
		});
	},

	executeSql: function(db, sql, values, callback, scope) {
		Ext.Error.raise("The executeSql function has not been implemented on this Ext.data.proxy.Sql subclass.");
	},

	serializeSqlExecution: function(db) {
		Ext.Error.raise("The serializeSqlExecution function has not been implemented on this Ext.data.proxy.Sql subclass.");
	},

	parallelizeSqlExecution: function(db) {
		Ext.Error.raise("The parallelizeSqlExecution function has not been implemented on this Ext.data.proxy.Sql subclass.");
	},

	buildCreateQuery: function(table, record, data) {
		var me = this,
			placeholders = [],
			columnData,
			columns,
			values,
			i, len;

		if (!table.uniqueIdStrategy) {
			delete data[table.idProperty];
		}
		columnData = me.getColumnData(table, data);
		columns = columnData.columns;
		values = columnData.values;
		for (i = 0, len = columns.length; i < len; i++) {
			placeholders.push('?');
		}

		return {
			sql: columns.length ?
				'INSERT INTO ' + table.name + ' (' + columns.join(', ') + ') VALUES (' + placeholders.join(', ') + ')' :
				'INSERT INTO ' + table.name + ' DEFAULT VALUES',
			values: values,
		};
	},

	buildUpdateQuery: function(table, record, data) {
		var me = this,
			id = record.getId(),
			columnData,
			columns,
			values,
			updates = [],
			i, len;

		delete data[table.idProperty];
		columnData = me.getColumnData(table, data);
		columns = columnData.columns;
		values = columnData.values.concat(id);
		for (i = 0, len = columns.length; i < len; i++) {
			updates.push(columns[i] + ' = ?');
		}

		return {
			sql: 'UPDATE ' + table.name + ' SET ' + updates.join(', ') + ' WHERE ' + table.idProperty + ' = ?',
			values: values,
		};
	},

	buildDestroyQuery: function(table, record) {
		var me = this,
			id = record.getId();

		return {
			sql: 'DELETE FROM ' + table.name + ' WHERE ' + table.idProperty + ' = ?',
			values: [id],
		};
	},

	getColumnData: function(table, data) {
		var columns = [],
			values = [];

		Ext.iterate(table.columns, function(field, column) {
			if (column in data) {
				columns.push(column);
				values.push(data[column]);
			}
		});
		return {
			columns: columns,
			values: values,
		};
	},

	getTableInfo: function(model) {
		var uniqueIdStrategy = this.getUniqueIdStrategy() || (model.identifier ? !!model.identifier.isUnique : false),
			idProperty = model.idField[this.getWriter().getNameProperty()] || model.idField.name,
			clientIdProperty = model.clientIdProperty,
			proto;

		if (!clientIdProperty) {
			clientIdProperty = 'clientId';
			proto = model.isInstance ? model.self.prototype : model.prototype;
			proto.clientIdProperty = clientIdProperty;
		}
		return {
			name: model.entityName,
			schemaString: this.getSchemaString(model),
			columns: this.getPersistedModelColumns(model),
			uniqueIdStrategy: uniqueIdStrategy,
			idProperty: idProperty,
			clientIdProperty: clientIdProperty,
			exists: false
		};
	},

	getSchemaString: function(model) {
		var me = this,
			schema = [],
			fields = model.getFields(),
			uniqueIdStrategy = model.identifier && model.identifier.isUnique,
			nameProperty = me.getWriter().getNameProperty(),
			field, type, name,
			i, ln;

		for (i = 0, ln = fields.length; i < ln; i++) {
			field = fields[i];
			type = field.getType();
			name = field[nameProperty] || field.name;

			if (!field.persist) {
				continue;
			}
			if (field.identifier) {
				if (uniqueIdStrategy) {
					type = me.convertToSqlType(type);
					schema.unshift(name + ' ' + type + ' PRIMARY KEY');
				} else {
					schema.unshift(name + ' INTEGER PRIMARY KEY AUTOINCREMENT');
				}
			} else {
				type = me.convertToSqlType(type);
				schema.push(name + ' ' + type);
			}
		}

		return schema.join(', ');
	},

	getPersistedModelColumns: function(model) {
		var me = this,
			fields = model.getFields(),
			nameProperty = me.getWriter().getNameProperty(),
			columns = {},
			i, len, field, name;

		for (i = 0, len = fields.length; i < len; i++) {
			field = fields[i];
			if (field.persist) {
				columns[field.name] = field[nameProperty] || field.name;
			}
		}
		return columns;
	},

	convertToSqlType: function(type) {
		switch (type.toLowerCase()) {
			case 'date':
			case 'string':
			case 'auto':
				return 'TEXT';
			case 'int':
				return 'INTEGER';
			case 'float':
				return 'REAL';
			case 'bool':
			case 'boolean':
				return 'NUMERIC';
			default:
				return 'TEXT';
		}
	},

	dropTable: function(model, callback, scope) {
		var me = this,
			db = me.getDatabaseObject(),
			table;
			
		if (!model) {
			table = me.getTable();
		}
		else if (!model.entityName && Ext.isFunction(model)) {
			table = me.getTable();
			callback = table;
			scope = callback;
		}
		else {
			table = me.getTable(model);
		}
		me.serializeSqlExecution(db, function(tx) {
			me.executeSql(tx, 'DROP TABLE IF EXISTS ' + table.name, null, function(error) {
				me.log('DROP TABLE IF EXISTS ' + table.name);
				table.exists = false;
				Ext.callback(callback, scope || me, [error !== null, table, error]);
			});
		});
	},
	
	clear: function(callback, scope) {
		var me = this,
			model = me.getModel(),
			models = {},
			childType,
			barrier;
			
		if (!model) {
			Ext.callback(callback, scope);
			return;
		}
		
		do {
			models[model.entityName] = model;
			childType = model.prototype.childType;
			model = childType && model.schema.getEntity(childType);
		} 
		while (model && !(childType in models));

		barrier = Ext.Function.createBarrier(models.length, function() {
			Ext.callback(callback, scope);
		});
		Ext.iterate(models, function(entityName, model) {
			me.dropTable(model, barrier);
		});
	},

	getDatabaseObject: function() {
		Ext.Error.raise("The getDatabaseObject function has not been implemented on this Ext.data.proxy.Sql subclass.");
	},

	destroy: function() {
		this.tables = null;
	},

	inheritableStatics: {

		databaseMap: {},

		getDatabaseObject: function(name) {
			var database = this.databaseMap[name];

			if (!database) {
				database = this.openDatabase(name);
				this.databaseMap[name] = database;
			}
			return database;
		},

		openDatabase: function() {
			Ext.Error.raise("The openDatabase function has not been implemented on this Ext.data.proxy.Sql subclass.");
		},

	}
});