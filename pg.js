const Database = require('pg');
const Lo = require('./pg-lo');
const POOLS = {};
const REG_ESCAPE_1 = /'/g;
const REG_ESCAPE_2 = /\\/g;
const REG_PARAMS = /\$\d/g;
const EMPTYARRAY = [];
const BLACKLIST = { dbms: 1 };
const ISOP = { '+': 1, '-': 1, '*': 1, '/': 1, '=': 1, '!': 1, '#': 1 };
const CANSTATS = global.F ? (global.F.stats && global.F.stats.performance && global.F.stats.performance.dbrm != null) : false;

// Convertor: Numeric to number
Database.types.setTypeParser(1700, val => val == null ? null : +val);

var ESCAPE = global.PG_ESCAPE = function(value) {

	if (value == null)
		return 'null';

	var type = typeof(value);

	if (type === 'function') {
		value = value();
		if (value == null)
			return 'null';
		type = typeof(value);
	}

	if (type === 'boolean')
		return value === true ? 'true' : 'false';

	if (type === 'number')
		return value.toString();

	if (type === 'string')
		return pg_escape(value);

	if (value instanceof Array)
		return pg_escape(value.join(','));

	if (value instanceof Date)
		return pg_escape(dateToString(value));

	return pg_escape(value.toString());
};

function createpool(opt) {
	return POOLS[opt.id] ? POOLS[opt.id] : (POOLS[opt.id] = opt.options.native ? new Database.native.Pool(opt.options) : new Database.Pool(opt.options));
}

function createclient(opt) {
	return opt.options.native ? new Database.native.Client(opt.options) : new Database.Client(opt.options);
}

function select(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var params = [];
	var q = 'SELECT ' + FIELDS(builder) + ' FROM ' + opt.table + WHERE(builder, null, null, params);

	F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);
	builder.db.$debug && builder.db.$debug(q);

	if (CANSTATS)
		F.stats.performance.dbrm++;

	client.query(q, params, function(err, response) {
		builder.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, q, builder);

		var rows = response ? response.rows : EMPTYARRAY;
		if (opt.first)
			rows = rows.length ? rows[0] : null;

		// checks joins
		if (!err && builder.$joins) {
			client.$dbms._joins(rows, builder);
			setImmediate(builder.db.$next);
		} else
			builder.$callback(err, rows);

	});
}

function check(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var params = [];
	var q = 'SELECT 1 FROM ' + opt.table + WHERE(builder, null, null, params);

	F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);

	if (CANSTATS)
		F.stats.performance.dbrm++;

	builder.db.$debug && builder.db.$debug(q);
	client.query(q, params, function(err, response) {
		builder.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, q, builder);
		var is = response && response.rows ? response.rows[0] != null : false;
		builder.$callback(err, is);
	});
}

function query(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;

	if (!cmd.value && builder.options.params)
		cmd.value = [];

	var q = cmd.query + WHERE(builder, null, null, cmd.value);
	builder.db.$debug && builder.db.$debug(q);

	if (CANSTATS)
		F.stats.performance.dbrm++;

	F.$events.dbms && EMIT('dbms', 'query', cmd.query, opt.db);
	client.query(q, cmd.value, function(err, response) {
		builder.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, q, builder);
		var rows = response ? response.rows : EMPTYARRAY;
		if (opt.first)
			rows = rows.length ? rows[0] : null;
		builder.$callback(err, rows);
	});
}

function command(client, sql, cmd) {
	cmd.db.$debug && cmd.db.$debug(sql);
	F.$events.dbms && EMIT('dbms', 'query', sql, cmd.db);

	if (CANSTATS)
		F.stats.performance.dbrm++;

	client.query(sql, function(err) {
		cmd.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, sql);
		cmd.db.$next(err);
	});
}

function list(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var params = [];
	var query =  WHERE(builder, true, null, params);
	var q;

	if (cmd.improved && builder.skip) {

		q = 'SELECT ' + FIELDS(builder) + ' FROM ' + opt.table + query + OFFSET(builder);
		F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);
		builder.db.$debug && builder.db.$debug(q);
		builder.db.busy = true;

		if (CANSTATS)
			F.stats.performance.dbrm++;

		client.query(q, params, function(err, response) {
			builder.db.busy = false;
			var rows = response ? response.rows : [];
			if (!err && builder.$joins) {
				client.$dbms._joins(rows, builder, rows.length);
				setImmediate(builder.db.$next);
			} else
				builder.$callback(err, rows, rows.length);
		});

	} else {
		q = 'SELECT COUNT(1)::int as dbmsvalue FROM ' + opt.table + query;
		builder.db.$debug && builder.db.$debug(q);
		F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);

		if (CANSTATS)
			F.stats.performance.dbrm++;

		client.query(q, params, function(err, response) {
			builder.db.busy = false;
			err && client.$opt.onerror && client.$opt.onerror(err, q, builder);

			var count = err ? 0 : response.rows && response.rows.length ? response.rows[0].dbmsvalue : 0;
			var fn = function(err, response) {
				builder.db.busy = false;

				var rows = response ? response.rows : [];

				// checks joins
				// client.$dbms._joins(rows, builder);
				if (!err && builder.$joins) {
					client.$dbms._joins(rows, builder, count);
					setImmediate(builder.db.$next);
				} else
					builder.$callback(err, rows, count);
			};

			if (count) {
				builder.db.busy = true;
				q = 'SELECT ' + FIELDS(builder) + ' FROM ' + opt.table + query + OFFSET(builder);
				builder.db.$debug && builder.db.$debug(q);
				F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);

				if (CANSTATS)
					F.stats.performance.dbrm++;

				client.query(q, params, fn);
			} else
				fn(err, null);
		});
	}
}

function scalar(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var params = [];
	var q;

	switch (cmd.scalar) {
		case 'avg':
		case 'min':
		case 'sum':
		case 'max':
		case 'count':
			q = 'SELECT ' + cmd.scalar.toUpperCase() + (cmd.scalar !== 'count' ? ('(' + (cmd.field || cmd.name) + ')') : '(1)') + '::int as dbmsvalue FROM ' + opt.table;
			break;
		case 'group':
			q = 'SELECT ' + cmd.name + ', ' + (cmd.field ? ('SUM(' + cmd.field + ')::numeric') : 'COUNT(1)::int') + ' as count FROM ' + opt.table;
			break;
	}

	q = q + WHERE(builder, false, cmd.scalar === 'group' ? cmd.name : null, params);
	builder.db.$debug && builder.db.$debug(q);
	F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);

	if (CANSTATS)
		F.stats.performance.dbrm++;

	client.query(q, params, function(err, response) {
		builder.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, q, builder);

		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		if (!cmd.field && cmd.scalar !== 'group')
			rows = rows.length ? (rows[0].dbmsvalue || 0) : 0;

		builder.$callback(err, rows);
	});
}

function abortcommands(client, builder) {
	while (builder.db.$commands.length) {
		var c = builder.db.$commands.shift();
		if (c && c.type === 'commit') {
			c.type = 'rollback';
			builder.db.$commands.unshift(c);
			break;
		}
	}
}

function insert(client, cmd) {

	var builder = cmd.builder;

	cmd.builder.options.transform && cmd.builder.options.transform(cmd.builder.value, cmd.builder.db.$output, cmd.builder.db.$lastoutput);

	var keys = Object.keys(builder.value);
	var params = [];
	var fields = [];
	var values = [];
	var index = 1;
	var opt = builder.options;

	for (var i = 0; i < keys.length; i++) {
		var key = keys[i];
		var val = builder.value[key];
		if (val === undefined || BLACKLIST[key])
			continue;

		if (builder.options.fields && builder.options.fields.length) {
			var skip = true;
			for (var j = 0; j < builder.options.fields.length; j++) {
				var field = builder.options.fields[j];
				if (field[0] === '-') {
					field = field.substring(1);
					if (field === key || (ISOP[key[0]] && field === key.substring(1))) {
						skip = true;
						break;
					}
					skip = false;
				} else if (field === key || (ISOP[key[0]] && field === key.substring(1))) {
					skip = false;
					break;
				} else
					skip = false;
			}

			if (skip)
				continue;
		}

		var raw = false;

		switch (key[0]) {
			case '-':
			case '+':
			case '*':
			case '/':
			case '>':
			case '<':
				key = key.substring(1);
				break;
			case '=':
				key = key.substring(1);
				raw = true;
				break;
			case '#':
				continue;
			case '!':
				// toggle
				key = key.substring(1);
				if (val)
					val = true;
				else
					val = false;
				break;
		}

		fields.push('"' + key + '"');

		if (raw) {
			values.push(val);
		} else {
			values.push('$' + index++);
			params.push(val == null ? null : typeof(val) === 'function' ? val(builder.value) : val);
		}
	}

	var q = 'INSERT INTO ' + opt.table + ' (' + fields.join(',') + ') VALUES(' + values.join(',') + ')' + (builder.$primarykey ? (' RETURNING ' + builder.$primarykey) : '');

	builder.db.$debug && builder.db.$debug(q);
	F.$events.dbms && EMIT('dbms', 'insert', opt.table, opt.db);

	if (CANSTATS)
		F.stats.performance.dbwm++;

	client.query(q, params, function(err, response) {

		// Transaction is aborted
		if (err && client.$dbmstransaction) {
			client.$dbmstransaction = true;
			abortcommands(client, builder);
		}

		builder.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, q, builder);
		builder.$callback(err, err == null ? (response.rows && response.rows.length ? response.rows[0][builder.$primarykey] : 1) : 0);
	});
}

function insertexists(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'SELECT 1 as dbmsvalue FROM ' + opt.table + WHERE(builder);
	builder.db.$debug && builder.db.$debug(q);
	F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);

	if (CANSTATS)
		F.stats.performance.dbrm++;

	client.query(q, function(err, response) {
		builder.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, q, builder);
		var rows = response ? response.rows : EMPTYARRAY;
		if (rows.length)
			builder.$callback(err, 0);
		else
			insert(client, cmd);
	});
}

function modify(client, cmd) {

	cmd.builder.options.transform && cmd.builder.options.transform(cmd.builder.value, cmd.builder.db.$output, cmd.builder.db.$lastoutput);

	var keys = Object.keys(cmd.builder.value);
	var fields = [];
	var params = [];
	var index = 1;

	for (var i = 0; i < keys.length; i++) {
		var key = keys[i];
		var val = cmd.builder.value[key];

		if (val === undefined || BLACKLIST[key])
			continue;

		if (cmd.builder.options.equal && cmd.builder.options.equal.indexOf(key) !== -1)
			continue;

		if (cmd.builder.options.fields && cmd.builder.options.fields.length) {
			var skip = true;
			for (var j = 0; j < cmd.builder.options.fields.length; j++) {
				var field = cmd.builder.options.fields[j];
				if (field[0] === '-') {
					field = field.substring(1);
					if (field === key || (ISOP[key[0]] && field === key.substring(1))) {
						skip = true;
						break;
					}
					skip = false;
				} else if (field === key || (ISOP[key[0]] && field === key.substring(1))) {
					skip = false;
					break;
				} else
					skip = false;
			}

			if (skip)
				continue;
		}

		var c = key[0];
		var type;

		if (typeof(val) === 'function')
			val = val(cmd.builder.value);

		switch (c) {
			case '-':
			case '+':
			case '*':
			case '/':
				params.push(val ? val : 0);
				key = key.substring(1);
				type = '"' + key + '"=COALESCE(' + key + ',0)' + c + '$' + (index++);
				break;
			case '>':
			case '<':
				params.push(val ? val : 0);
				key = key.substring(1);
				type = '"' + key + '"=' + (c === '>' ? 'GREATEST' : 'LEAST') + '(' + key + ',$' + (index++) + ')';
				break;
			case '!':
				// toggle
				key = key.substring(1);
				type = '"' + key + '"=NOT ' + key;
				break;
			case '=':
			case '#':
				// raw
				type = '"' + key.substring(1) + '"=' + val;
				break;
			default:
				params.push(val);
				type = '"' + key + '"=$' + (index++);
				break;
		}
		type && fields.push(type);
	}

	var builder = cmd.builder;
	var opt = builder.options;

	if (opt.equal) {
		for (var i = 0; i < opt.equal.length; i++)
			cmd.builder.where(opt.equal[i], builder.value[opt.equal[i]]);
	}

	var q = 'WITH rows AS (UPDATE ' + opt.table + ' SET ' + fields + WHERE(builder, true, null, params) + ' RETURNING 1) SELECT count(1)::int as dbmsvalue FROM rows';
	builder.db.$debug && builder.db.$debug(q);
	F.$events.dbms && EMIT('dbms', 'update', opt.table, opt.db);

	if (CANSTATS)
		F.stats.performance.dbwm++;

	client.query(q, params, function(err, response) {

		// Transaction is aborted
		if (err && client.$dbmstransaction) {
			client.$dbmstransaction = true;
			abortcommands(client, builder);
		}

		builder.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, q, builder);
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		rows = rows.length ? rows[0].dbmsvalue : 0;
		if (!rows && cmd.insert) {
			if (cmd.insert !== true)
				cmd.builder.value = cmd.insert;
			cmd.builder.options.insert && cmd.builder.options.insert(cmd.builder.value, cmd.builder.options.insertparams);
			insert(client, cmd);
		} else
			builder.$callback(err, rows);
	});
}

function remove(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var params = [];
	var q = 'WITH rows AS (DELETE FROM ' + opt.table + WHERE(builder, true, null, params) + ' RETURNING 1) SELECT count(1)::int as dbmsvalue FROM rows';
	builder.db.$debug && builder.db.$debug(q);
	F.$events.dbms && EMIT('dbms', 'delete', opt.table, opt.db);

	if (CANSTATS)
		F.stats.performance.dbwm++;

	client.query(q, params, function(err, response) {

		// Transaction is aborted
		if (err && client.$dbmstransaction) {
			client.$dbmstransaction = true;
			abortcommands(client, builder);
		}

		builder.db.busy = false;
		err && client.$opt.onerror && client.$opt.onerror(err, q, builder);
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		rows = rows.length ? rows[0].dbmsvalue : 0;
		builder.$callback(err, rows);
	});
}

function destroy(conn) {

	var client = conn.client;
	if (client) {

		if (client.$dbmstransaction)
			client.$dbmstransaction = false;

		if (client.release)
			client.release();
		else
			client.end();
	}
}

function clientcommand(cmd, client, self) {
	switch (cmd.type) {
		case 'transaction':
			client.$dbmstransaction = true;
			command(client, 'BEGIN', cmd);
			break;
		case 'end':
			cmd.type = self.$eb ? self.$errors.items.length ? 'ROLLBACK' : self.$errors.length ? 'ROLLBACK' : 'COMMIT' : 'COMMIT';
			command(client, cmd.type, cmd);
			break;
		case 'commit':
		case 'rollback':
			client.$dbmstransaction = false;
			command(client, cmd.type.toUpperCase(), cmd);
			break;
		case 'find':
		case 'read':
			select(client, cmd);
			break;
		case 'diff':
			var cb = cmd.builder.$callback;
			cmd.builder.$callback = function(err, response) {
				cb.call(cmd.builder, err, err ? EMPTYOBJECT : DIFFARR(cmd.key, response, cmd.form));
			};
			select(client, cmd);
			break;

		case 'modify2':
			var cb = cmd.builder.$callback;
			cmd.builder.$callback = function(err, response) {
				cmd.builder.options.fields = null;
				if (err) {
					cb.call(cmd.builder, err, 0);
				} else if (response) {
					cmd.builder.db.busy = true;
					var mod = cmd.fn(response, cmd.builder.db.$output, cmd.builder.db.$outputall);
					cmd.builder.db.busy = false;
					if (mod) {
						cmd.builder.value = mod;
						cmd.builder.$callback = cb;
						if (cmd.builder.value.$clean)
							cmd.builder.value = cmd.builder.value.$clean();
						modify(client, cmd);
					} else
						cb.call(cmd.builder, err, 0);
				} else {
					if (cmd.insert) {
						cmd.builder.db.busy = true;
						mod = cmd.fn(null, cmd.builder.db.$output, cmd.builder.db.$outputall);
						cmd.builder.db.busy = false;
						if (mod) {
							cmd.builder.value = mod;
							cmd.builder.$callback = cb;
							insert(client, cmd);
						} else
							cb.call(cmd.builder, err, 0);
					} else {
						cb.call(cmd.builder, err, 0);
					}
				}
			};
			select(client, cmd);
			break;
		case 'check':
			check(client, cmd);
			break;
		case 'list':
			list(client, cmd);
			break;
		case 'scalar':
			scalar(client, cmd);
			break;
		case 'insert':
			if (cmd.unique)
				insertexists(client, cmd);
			else
				insert(client, cmd);
			break;
		case 'update':
		case 'modify':
			modify(client, cmd);
			break;
		case 'remove':
			remove(client, cmd);
			break;
		case 'query':
			query(client, cmd);
			break;
		default:
			cmd.builder.$callback(new Error('Operation "' + cmd.type + '" not found'));
			break;
	}
}

exports.run = function(opt, self, cmd, repeated) {

	var conn = self.$conn[opt.id];

	if (!conn) {
		conn = self.$conn[opt.id] = { driver: opt.options.pooling ? createpool(opt) : createclient(opt) };
		conn.$$destroy = destroy;
	}

	self.$op = null;
	self.busy = true;

	if (conn.client) {
		clientcommand(cmd, conn.client, self);
		return;
	}

	conn.driver.connect(function(err, client) {

		if (err) {

			self.busy = false;
			opt.onerror && opt.onerror(err);

			if ((!repeated || repeated < 3) && err.toString().indexOf('many clients') !== -1) {
				// try again
				setTimeout(function() {
					exports.run(opt, self, cmd, (repeated || 0) + 1);
				}, 200);
				return;
			}

			if (cmd.builder)
				cmd.builder.$callback(err);
			else
				cmd.db.$next(err);

		} else {
			conn.client = client;
			client.$opt = opt;
			client.$dbms = self;
			clientcommand(cmd, conn.client, self);
		}
	});
};

exports.blob_remove = function(opt, id, callback) {
	if (typeof(id) === 'string')
		id = +id;
	createpool(opt).connect(function(err, client, done) {
		if (err) {
			done && done();
			callback && callback(err);
		} else {
			client.query('DELETE FROM pg_largeobject WHERE loid=' + id, function(err) {
				done();
				callback && callback(err);
			});
		}
	});
};

exports.blob_read = function(opt, id, callback) {

	if (typeof(id) === 'string')
		id = +id;

	createpool(opt).connect(function(err, client, done) {

		if (err)
			return callback(err);

		if (CANSTATS)
			F.stats.performance.dbrm++;

		client.query('BEGIN', function(err) {

			if (err) {
				done();
				return callback(err);
			}

			if (CANSTATS)
				F.stats.performance.dbrm++;

			Lo.create(client).readStream(id, opt.buffersize || 16384, function(err, size, stream) {
				if (err) {
					client.query('COMMIT', done);
					callback(err);
				} else {
					var cb = () => client.query('COMMIT', done);
					stream.on('error', cb);
					stream.on('end', cb);
					callback(null, stream, { size: parseInt(size) });
				}
			});
		});
	});
};

exports.blob_write = function(opt, stream, name, callback) {

	if (typeof(name) === 'function') {
		callback = name;
		name = undefined;
	}

	createpool(opt).connect(function(err, client, done) {

		if (err)
			return callback(err);

		if (CANSTATS)
			F.stats.performance.dbrm++;

		client.query('BEGIN', function(err) {

			if (err) {
				done();
				return callback(err);
			}

			if (CANSTATS)
				F.stats.performance.dbwm++;

			Lo.create(client).writeStream(opt.buffersize || 16384, function(err, oid, writer) {

				if (err) {
					client.query('ROLLBACK', done);
					return callback(err);
				}

				writer.on('finish', function() {
					client.query('COMMIT', done);
					callback(null, oid.toString());
				});

				stream.pipe(writer);
			});
		});
	});
};

function prepare_owner(cmd, condition) {

	var tmp = [];

	if (cmd.member) {
		for (var i = 0; i < cmd.member.length; i++)
			tmp.push(ESCAPE(cmd.member[i]));
	}

	var addcondition = [];
	var keys = cmd.condition ? Object.keys(cmd.condition) : null;
	if (keys) {
		addcondition.push('');
		for (var i = 0; i < keys.length; i++) {
			var val = cmd.condition[keys[i]];
			addcondition.push(keys[i] + (val == null ? ' IS ' : '=') + ESCAPE(val));
		}
	}

	// e.g. userid=ID OR (userid IN (ARR) (AND condition))
	condition.push('(' + cmd.name + '=' + ESCAPE(cmd.value) + (tmp.length ? (' OR (' + cmd.name + ' IN (' + tmp.join(',') + ')' + addcondition.join(' AND ') + ')') : '') + ')');
}

function WHERE(builder, scalar, group, params) {

	var condition = [];
	var sort = [];
	var tmp;
	var op = 'AND';
	var opuse = false;
	var index = -1;
	var current; // temporary for "query" + "cmd.value" and "replace" method because of performance

	var replace = builder.options.params ? function(text) {
		var indexer = (+text.substring(1)) - 1;
		index = params.push(current[indexer]);
		return '$' + index;
	} : null;

	for (var i = 0; i < builder.$commands.length; i++) {
		var cmd = builder.$commands[i];

		if (builder.options.islanguage && cmd.name && cmd.name[cmd.name.length - 1] === '§')
			cmd.name = cmd.name.substring(0, cmd.name.length - 1) + (builder.options.language || '');

		switch (cmd.type) {
			case 'where':
				if (cmd.value === undefined) {
					opuse && condition.length && condition.push(op);
					condition.push(cmd.name);
				} else {
					tmp = ESCAPE(cmd.value);
					opuse && condition.length && condition.push(op);
					condition.push(cmd.name + ((cmd.value == null || tmp == 'null') && cmd.compare === '=' ? ' IS ' : cmd.compare) + tmp);
				}
				break;

			case 'owner':
				opuse && condition.length && condition.push(op);
				prepare_owner(cmd, condition);
				break;

			case 'custom':
				cmd.fn.call(builder, builder, builder.db.$output, builder.db.$lastoutput);
				break;

			case 'in':
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				if (cmd.value instanceof Array) {
					tmp = [];
					for (var j = 0; j < cmd.value.length; j++) {
						var val = cmd.value[j];
						if (val && cmd.field)
							val = val[cmd.field];
						tmp.push(ESCAPE(val));
					}
					opuse && condition.length && condition.push(op);
					condition.push(cmd.name + ' IN (' + (tmp.length ? tmp.join(',') : 'NULL') + ')');
				} else {
					opuse && condition.length && condition.push(op);
					condition.push(cmd.name + '=' + ESCAPE(cmd.field ? cmd.value[cmd.field] : cmd.value));
				}
				break;

			case 'notin':
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				if (cmd.value instanceof Array) {
					tmp = [];
					for (var j = 0; j < cmd.value.length; j++) {
						var val = cmd.value[j];
						if (val && cmd.field)
							val = val[cmd.field];
						tmp.push(ESCAPE(val));
					}
					opuse && condition.length && condition.push(op);
					condition.push(cmd.name + ' NOT IN (' + (tmp.length ? tmp.join(',') : 'NULL') + ')');
				} else {
					opuse && condition.length && condition.push(op);
					condition.push(cmd.name + '<>' + ESCAPE(cmd.field ? cmd.value[cmd.field] : cmd.value));
				}
				break;

			case 'between':
				opuse && condition.length && condition.push(op);
				condition.push('(' + cmd.name + '>=' + ESCAPE(cmd.a) + ' AND ' + cmd.name + '<=' + ESCAPE(cmd.b) + ')');
				break;

			case 'search':
				tmp = ESCAPE((!cmd.compare || cmd.compare === '*' ? ('%' + cmd.value + '%') : (cmd.compare === 'beg' ? ('%' + cmd.value) : (cmd.value + '%'))));
				opuse && condition.length && condition.push(op);
				condition.push(cmd.name + ' ILIKE ' + tmp);
				break;

			case 'searchfull':
				tmp = ESCAPE('%' + cmd.value.toLowerCase().replace(/y/g, 'i') + '%');
				opuse && condition.length && condition.push(op);
				condition.push('REPLACE(LOWER(to_tsvector(' + builder.options.table + '::text)::text), \'y\', \'i\') ILIKE ' + tmp);
				break;

			case 'searchall':
				tmp = '';
				for (var j = 0; j < cmd.value.length; j++)
					tmp += (tmp ? ' AND ' : '') + cmd.name + ' ILIKE ' + ESCAPE('%' + cmd.value[j] + '%');
				opuse && condition.length && condition.push(op);
				condition.push('(' + (tmp || '0=1') + ')');
				break;

			case 'fulltext':
				tmp = ESCAPE('%' + cmd.value.toLowerCase() + '%');
				opuse && condition.length && condition.push(op);
				condition.push('LOWER(' + cmd.name + ') ILIKE ' + tmp);
				break;
			case 'contains':
				opuse && condition.length && condition.push(op);
				condition.push('LENGTH(' + cmd.name +'::text)>0');
				break;
			case 'query':
				opuse && condition.length && condition.push(op);
				if (cmd.value)
					current = cmd.value;
				condition.push('(' + (current == undefined ? cmd.query : cmd.query.replace(REG_PARAMS, replace)) + ')');
				break;
			case 'permit':
				opuse && condition.length && condition.push(op);
				if (cmd.must)
					condition.push('(' + ((cmd.useridfield ? ('"' + cmd.useridfield + '"=' + pg_escape(cmd.userid) + ' OR ') : '') + '"' + cmd.name + '" && $' + params.push([cmd.value])) + ')');
				else
					condition.push('(' + ((cmd.useridfield ? ('"' + cmd.useridfield + '"=' + pg_escape(cmd.userid) + ' OR ') : '') + 'array_length("' + cmd.name + '",1) IS NULL OR "' + cmd.name + '" && $' + params.push([cmd.value])) + ')');
				break;
			case 'empty':
				opuse && condition.length && condition.push(op);
				condition.push('(' + cmd.name + ' IS NULL OR LENGTH(' + cmd.name + '::text)=0)');
				break;
			case 'month':
			case 'year':
			case 'day':
			case 'hour':
			case 'minute':
				opuse && condition.length && condition.push(op);
				condition.push('EXTRACT(' + cmd.type + ' from ' + cmd.name + ')' + cmd.compare + ESCAPE(cmd.value));
				break;
			case 'date':
				opuse && condition.length && condition.push(op);
				condition.push(cmd.name + '::date' + cmd.compare + cmd.value instanceof Date ? (cmd.value.format('yyyy-MM-dd') + '::date') : 'null');
				break;
			case 'or':
				opuse && condition.length && condition.push(op);
				op = 'OR';
				opuse = false;
				condition.push('(');
				continue;
			case 'end':
				condition.push(')');
				op = 'AND';
				break;
			case 'and':
				opuse && condition.length && condition.push(op);
				op = 'AND';
				break;
			case 'sort':
				sort.push(cmd.name + ' ' + (cmd.desc ? 'DESC' : 'ASC'));
				break;
			case 'regexp':
				tmp = cmd.value.toString().substring(1);
				var g = '~';
				if (tmp[tmp.length - 1] === 'i') {
					tmp = tmp.substring(0, tmp.length - 2);
					g = '~*';
				} else
					tmp = tmp.substring(0, tmp.length - 1);
				opuse && condition.length && condition.push(op);
				condition.push(cmd.name + g + '\'' + tmp + '\'');
				break;
		}
		opuse = true;
	}

	var query = (condition.length ? (' WHERE ' + condition.join(' ')) : '') + (group ? (' GROUP BY ' + group) : '');

	if (scalar) {
		builder.options.sort = sort;
	} else {
		if (sort.length)
			query += ' ORDER BY ' + sort.join(',');
		if (builder.options.skip && builder.options.take)
			query += ' LIMIT ' + builder.options.take + ' OFFSET ' + builder.options.skip;
		else if (builder.options.take)
			query += ' LIMIT ' + builder.options.take;
		else if (builder.options.skip)
			query += ' OFFSET ' + builder.options.skip;
	}

	return query;
}

function OFFSET(builder) {
	var query = '';
	var sort = builder.options.sort || EMPTYARRAY;
	if (sort.length)
		query += ' ORDER BY ' + sort.join(',');
	if (builder.options.skip && builder.options.take)
		query += ' LIMIT ' + builder.options.take + ' OFFSET ' + builder.options.skip;
	else if (builder.options.take)
		query += ' LIMIT ' + builder.options.take;
	else if (builder.options.skip)
		query += ' OFFSET ' + builder.options.skip;
	return query;
}

function FIELDS(builder) {

	var output = '';
	var plus = '';
	var fields = builder.options.fields;

	if (fields && fields.length) {
		for (var i = 0; i < fields.length; i++) {
			var field = fields[i];
			if (field[0] === '-') {

				if (builder.options.fieldsrem)
					builder.options.fieldsrem.push(field.substring(1));
				else
					builder.options.fieldsrem = [field.substring(1)];

				continue;
			}

			if (builder.options.islanguage) {
				if (field[field.length - 1] === '§') {
					field = field.substring(0, field.length - 1);
					field = (field + builder.options.language) + ' AS ' + field;
				}
			}

			output += (output ? ',' : '') + field;
		}
		if (output && builder.$joinmeta)
			output += ',' + builder.$joinmeta.a;
	}

	fields = builder.options.subquery;
	if (fields && fields.length) {
		for (var i = 0; i < fields.length; i++)
			plus += ',' + (fields[i].name ? ('(' + fields[i].query + ') AS ' + fields[i].name) : fields[i].query);
	}

	return (output ? output : '*') + plus;
}

// Author: https://github.com/segmentio/pg-escape
// License: MIT
function pg_escape(val) {
	if (val == null)
		return 'NULL';
	var backslash = ~val.indexOf('\\');
	var prefix = backslash ? 'E' : '';
	val = val.replace(REG_ESCAPE_1, '\'\'').replace(REG_ESCAPE_2, '\\\\');
	return prefix + '\'' + val + '\'';
}

function dateToString(dt) {

	var arr = [];

	arr.push(dt.getFullYear().toString());
	arr.push((dt.getMonth() + 1).toString());
	arr.push(dt.getDate().toString());
	arr.push(dt.getHours().toString());
	arr.push(dt.getMinutes().toString());
	arr.push(dt.getSeconds().toString());

	for (var i = 1, length = arr.length; i < length; i++) {
		if (arr[i].length === 1)
			arr[i] = '0' + arr[i];
	}

	return arr[0] + '-' + arr[1] + '-' + arr[2] + ' ' + arr[3] + ':' + arr[4] + ':' + arr[5];
}