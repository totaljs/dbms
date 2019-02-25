const Database = require('pg');
const Lo = require('./pg-lo');
const POOLS = {};
const REG_ESCAPE_1 = /'/g;
const REG_ESCAPE_2 = /\\/g;
const EMPTYARRAY = [];
const BLACKLIST = { dbms: 1 };

function createpool(opt) {
	return POOLS[opt.id] ? POOLS[opt.id] : (POOLS[opt.id] = opt.options.native ? new Database.native.Pool(opt.options) : new Database.Pool(opt.options));
}

function createclient(opt) {
	return opt.options.native ? new Database.native.Client(opt.options) : new Database.Client(opt.options);
}

function select(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'SELECT ' + FIELDS(builder) + ' FROM ' + opt.table + WHERE(builder);

	builder.db.$debug && builder.db.$debug(q);
	client.query(q, function(err, response) {

		client.$done();
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

function query(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var q = cmd.query + WHERE(builder);
	builder.db.$debug && builder.db.$debug(q);
	client.query(q, cmd.value, function(err, response) {
		client.$done();
		var rows = response ? response.rows : EMPTYARRAY;
		if (opt.first)
			rows = rows.length ? rows[0] : null;
		builder.$callback(err, rows);
	});
}

function command(client, sql, cmd) {
	cmd.db.$debug && cmd.db.$debug(sql);
	client.query(sql, function(err) {
		client.$done();
		cmd.db.$next(err);
	});
}

function list(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'SELECT COUNT(1)::int as dbmsvalue FROM ' + opt.table + WHERE(builder, true) + ';SELECT ' + FIELDS(builder) + ' FROM ' + opt.table + WHERE(builder);

	builder.db.$debug && builder.db.$debug(q);

	client.query(q, function(err, response) {

		client.$done();

		var rows, meta;

		if (response instanceof Array) {
			rows = response[1] ? (response[1].rows || EMPTYARRAY) : EMPTYARRAY;
			meta = response[0] ? response[0].rows[0] : null;
		} else {
			rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
			meta = rows.shift();
		}

		// checks joins
		// client.$dbms._joins(rows, builder);
		if (!err && builder.$joins) {
			client.$dbms._joins(rows, builder, meta ? meta.dbmsvalue || 0 : 0);
			setImmediate(builder.db.$next);
		} else
			builder.$callback(err, rows, meta ? meta.dbmsvalue || 0 : 0);
	});
}

function scalar(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var q;

	switch (cmd.scalar) {
		case 'avg':
		case 'min':
		case 'sum':
		case 'max':
		case 'count':
			q = 'SELECT ' + cmd.scalar.toUpperCase() + (cmd.scalar !== 'count' ? ('(' + cmd.name + ')') : '(1)') + '::int as dbmsvalue FROM ' + opt.table;
			break;
		case 'group':
			q = 'SELECT ' + cmd.name + ', COUNT(1)::int as count FROM ' + opt.table;
			break;
	}

	q = q + WHERE(builder, false, cmd.scalar === 'group' ? cmd.name : null);
	builder.db.$debug && builder.db.$debug(q);

	client.query(q, function(err, response) {
		client.$done();
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		if (cmd.scalar !== 'group')
			rows = rows.length ? (rows[0].dbmsvalue || 0) : 0;
		builder.$callback(err, rows);
	});
}

function insert(client, cmd) {

	var builder = cmd.builder;
	var keys = Object.keys(cmd.builder.value);
	var params = [];
	var fields = [];
	var values = [];
	var index = 1;
	var opt = builder.options;

	for (var i = 0; i < keys.length; i++) {
		var key = keys[i];
		var val = cmd.builder.value[key];
		if (val === undefined || BLACKLIST[key])
			continue;

		if (cmd.builder.options.fields && cmd.builder.options.fields.length) {
			var skip = true;
			for (var j = 0; j < cmd.builder.options.fields.length; j++) {
				if (cmd.builder.options.fields[j] == key || cmd.builder.options.fields[j] == key.substring(1)) {
					skip = false;
					break;
				}
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
				key = key.substring(1);
				break;
			case '=':
				key = key.substring(1);
				raw = true;
				break;
			case '!':
				// toggle
				key = key.substring(1);
				if (val)
					val = true;
				else
					val = false;
				break;
		}

		fields.push(key);

		if (raw) {
			values.push(val);
		} else {
			values.push('$' + index++);
			params.push(val == null ? null : typeof(val) === 'function' ? val(cmd.builder.value) : val);
		}
	}

	var q = 'INSERT INTO ' + opt.table + ' (' + fields.join(',') + ') VALUES(' + values.join(',') + ')';

	builder.db.$debug && builder.db.$debug(q);
	client.query(q, params, function(err) {
		client.$done();
		builder.$callback(err, err == null ? 1 : 0);
	});
}

function insertexists(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'SELECT 1 as dbmsvalue FROM ' + opt.table + WHERE(builder);
	builder.db.$debug && builder.db.$debug(q);
	client.query(q, function(err, response) {
		var rows = response ? response.rows : EMPTYARRAY;
		if (rows.length)
			builder.$callback(err, 0);
		else
			insert(client, cmd);
	});
}

function modify(client, cmd) {

	var keys = Object.keys(cmd.builder.value);
	var fields = [];
	var params = [];
	var index = 1;

	for (var i = 0; i < keys.length; i++) {
		var key = keys[i];
		var val = cmd.builder.value[key];

		if (val === undefined || BLACKLIST[key])
			continue;

		if (cmd.builder.options.fields && cmd.builder.options.fields.length) {
			var skip = true;
			for (var j = 0; j < cmd.builder.options.fields.length; j++) {
				if (cmd.builder.options.fields[j] == key || cmd.builder.options.fields[j] == key.substring(1)) {
					skip = false;
					break;
				}
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
				type = key + '=COALESCE(' + key + ',0)' + c + '$' + (index++);
				break;
			case '!':
				// toggle
				key = key.substring(1);
				type = key + '=NOT ' + key;
				break;
			case '=':
				// raw
				type = key.substring(1) + '=' + val;
				break;
			default:
				params.push(val);
				type = key + '=$' + (index++);
				break;
		}
		type && fields.push(type);
	}

	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'WITH rows AS (UPDATE ' + opt.table + ' SET ' + fields + WHERE(builder, true) + ' RETURNING 1) SELECT count(1)::int as dbmsvalue FROM rows';

	builder.db.$debug && builder.db.$debug(q);
	client.query(q, params, function(err, response) {
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		rows = rows.length ? rows[0].dbmsvalue : 0;
		if (!rows && cmd.insert) {
			if (cmd.insert !== true)
				cmd.builder.value = cmd.insert;
			cmd.builder.options.insert && cmd.builder.options.insert(cmd.builder.value);
			insert(client, cmd);
		} else {
			client.$done();
			builder.$callback(err, rows);
		}
	});
}

function remove(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'WITH rows AS (DELETE FROM ' + opt.table + WHERE(builder, true) + ' RETURNING 1) SELECT count(1)::int as dbmsvalue FROM rows';
	builder.db.$debug && builder.db.$debug(q);
	client.query(q, function(err, response) {
		client.$done();
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		rows = rows.length ? rows[0].dbmsvalue : 0;
		builder.$callback(err, rows);
	});
}

exports.run = function(opt, self, cmd) {
	var conn = opt.options.pooling ? createpool(opt) : createclient(opt);
	conn.connect(function(err, client, done) {
		if (err) {
			if (cmd.builder)
				cmd.builder.$callback(err);
			else
				cmd.db.$next(err);
		} else {
			client.$dbms = self;
			client.$done = done || client.end;
			switch (cmd.type) {
				case 'transaction':
					command(client, 'BEGIN', cmd);
					break;
				case 'commit':
				case 'rollback':
					command(client, cmd.type.toUpperCase(), cmd);
					break;
				case 'find':
				case 'read':
					select(client, cmd);
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
					done();
					break;
			}
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

		client.query('BEGIN', function(err) {

			if (err) {
				done();
				return callback(err);
			}

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

		client.query('BEGIN', function(err) {

			if (err) {
				done();
				return callback(err);
			}

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

function WHERE(builder, scalar, group) {

	var condition = [];
	var sort = [];
	var tmp;
	var op = 'AND';
	var opuse = false;

	for (var i = 0; i < builder.$commands.length; i++) {
		var cmd = builder.$commands[i];
		switch (cmd.type) {
			case 'where':
				tmp = ESCAPE(cmd.value);
				opuse && condition.length && condition.push(op);
				condition.push(cmd.name + (cmd.value == null && cmd.compare === '=' ? ' IS ' : cmd.compare) + tmp);
				break;
			case 'in':
				opuse && condition.length && condition.push(op);
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				if (cmd.value instanceof Array) {
					tmp = [];
					for (var j = 0; j < cmd.value.length; j++)
						tmp.push(ESCAPE(cmd.value[j]));
					condition.push(cmd.name + ' IN (' + tmp.join(',') + ')');
				} else
					condition.push(cmd.name + '=' + ESCAPE(cmd.value));
				break;
			case 'notin':
				opuse && condition.length && condition.push(op);
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				if (cmd.value instanceof Array) {
					tmp = [];
					for (var j = 0; j < cmd.value.length; j++)
						tmp.push(ESCAPE(cmd.value[j]));
					condition.push(cmd.name + ' NOT IN (' + tmp.join(',') + ')');
				} else
					condition.push(cmd.name + '<>' + ESCAPE(cmd.value));
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
			case 'fulltext':
				tmp = ESCAPE('%' + cmd.value.toLowerCase() + '%');
				opuse && condition.length && condition.push(op);
				condition.push('LOWER(' + cmd.name + ') ILIKE ' + tmp);
				break;
			case 'contains':
				opuse && condition.length && condition.push(op);
				condition.push('LENGTH(' + cmd.name + +'::text)>0');
				break;
			case 'empty':
				opuse && condition.length && condition.push(op);
				condition.push('(' + cmd.name + ' IS NULL OR LENGTH(' + cmd.name + +'::text)=0)');
				break;
			case 'month':
			case 'year':
			case 'day':
			case 'hour':
			case 'minute':
				opuse && condition.length && condition.push(op);
				condition.push('EXTRACT(' + cmd.type + ' from ' + cmd.name + ')' + cmd.compare + ESCAPE(cmd.value));
				break;
			case 'code':
				opuse && condition.length && condition.push(op);
				condition.push('(' + cmd.value + ')');
				break;
			case 'or':
				opuse && condition.length && condition.push(op);
				op = 'OR';
				opuse = false;
				condition.push('(');
				continue;
			case 'end':
				condition.push(')');
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

	if (!scalar) {
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

function FIELDS(builder) {
	var output = '';
	var fields = builder.options.fields;
	if (fields && fields.length) {
		for (var i = 0; i < fields.length; i++)
			output += (output ? ',' : '') + fields[i];
		if (builder.$joinmeta)
			output += ',' + builder.$joinmeta.a;
	}
	return output ? output : '*';
}

function ESCAPE(value) {

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
}

// Author: https://github.com/segmentio/pg-escape
// License: MIT
function pg_escape(val){
	if (val === null)
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