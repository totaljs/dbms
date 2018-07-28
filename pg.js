// @TODO: insert + unique

const Database = require('pg');
const POOLS = {};
const REG_ESCAPE_1 = /'/g;
const REG_ESCAPE_2 = /\\/g;
const EMPTYARRAY = [];
const SCOL = '"';

function createpool(opt) {
	return POOLS[opt.id] ? POOLS[opt.id] : (POOLS[opt.id] = opt.options.native ? new Database.native.Pool(opt.options) : new Database.Pool(opt.options));
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
		builder.$callback(err, rows);
	});
}

function listing(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'SELECT COUNT(*)::int as dbmsvalue FROM ' + opt.table + WHERE(builder, true) + ';SELECT ' + FIELDS(builder) + ' FROM ' + opt.table + WHERE(builder);

	builder.db.$debug && builder.db.$debug(q);

	client.query(q, function(err, response) {
		client.$done();
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		var meta = rows.shift();
		builder.$callback(err, rows, meta.dbmsvalue || 0);
	});
}

function count(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'SELECT COUNT(*)::int as dbmsvalue FROM ' + opt.table + WHERE(builder);

	builder.db.$debug && builder.db.$debug(q);

	client.query(q, function(err, response) {
		client.$done();
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		rows = rows.length ? rows[0].dbmsvalue : 0;
		builder.$callback(err, rows);
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
			q = 'SELECT ' + cmd.scalar.toUpperCase() + '("' + cmd.name + '")::int as dbmsvalue FROM ' + opt.table;
			break;
		case 'group':
			q = 'SELECT "' + cmd.name + '" FROM ' + opt.table;
			break;
	}

	q = q + WHERE(builder, false, cmd.scalar === 'group' ? cmd.name : null);
	builder.db.$debug && builder.db.$debug(q);

	client.query(q, function(err, response) {
		client.$done();
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		if (cmd.scalar !== 'group')
			rows = rows[0].dbmsvalue || 0;
		builder.$callback(err, rows);
	});
}

function insert(client, cmd) {

	var builder = cmd.builder;
	var keys = Object.keys(cmd.value);
	var params = [];
	var fields = [];
	var values = [];
	var index = 1;
	var opt = builder.options;

	for (var i = 0; i < keys.length; i++) {
		var key = keys[i];
		var val = cmd.value[key];
		if (val === undefined)
			continue;

		fields.push(SCOL + key + SCOL);
		values.push('$' + index++);
		params.push(val == null ? null : typeof(val) === 'function' ? val(cmd.value) : val);
	}

	var q = 'INSERT INTO ' + opt.table + ' (' + fields.join(',') + ') VALUES(' + values.join(',') + ')';

	builder.db.$debug && builder.db.$debug(q);
	client.query(q, params, function(err) {
		client.$done();
		builder.$callback(err, err == null ? 1 : 0);
	});
}

function modify(client, cmd) {

	var keys = Object.keys(cmd.value);
	var fields = [];
	var params = [];
	var index = 1;

	for (var i = 0; i < keys.length; i++) {
		var key = keys[i];
		var val = cmd.value[key];

		if (val === undefined)
			continue;

		var c = key[0];
		var type;

		if (typeof(val) === 'function')
			val = val(cmd.value);

		switch (c) {
			case '-':
			case '+':
			case '*':
			case '/':
				params.push(val ? val : 0);
				type = SCOL + key.substring(1) + '"=COALESCE("' + key + '",0)' + c + '$' + (index++);
				break;
			default:
				params.push(val);
				type = SCOL + key + '"=$' + (index++);
				break;
		}
		type && fields.push(type);
	}

	var builder = cmd.builder;
	var opt = builder.options;
	var q = 'WITH rows AS (UPDATE ' + opt.table + ' SET ' + fields + WHERE(builder, true) + ' RETURNING 1) SELECT count(*)::int as dbmsvalue FROM rows';

	builder.db.$debug && builder.db.$debug(q);
	client.query(q, params, function(err, response) {
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		rows = rows.length ? rows[0].dbmsvalue : 0;
		if (!rows && cmd.insert) {
			if (cmd.insert !== true)
				cmd.value = cmd.insert;
			cmd.builder.options.insert && cmd.builder.options.insert(cmd.value);
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
	var q = 'WITH rows AS (DELETE FROM ' + opt.table + WHERE(builder, true) + ' RETURNING 1) SELECT count(*)::int as dbmsvalue FROM rows';
	builder.db.$debug && builder.db.$debug(q);
	client.query(q, function(err, response) {
		client.$done();
		var rows = response ? response.rows || EMPTYARRAY : EMPTYARRAY;
		rows = rows.length ? rows[0].dbmsvalue : 0;
		builder.$callback(err, rows);
	});
}

exports.run = function(opt, self, cmd) {
	createpool(opt).connect(function(err, client, done) {
		if (err) {
			cmd.builder.$callback(err);
		} else {
			client.$done = done;
			switch (cmd.type) {
				case 'find':
					select(client, cmd);
					break;
				case 'listing':
					listing(client, cmd);
					break;
				case 'count':
					count(client, cmd);
					break;
				case 'scalar':
					scalar(client, cmd);
					break;
				case 'insert':
					insert(client, cmd);
					break;
				case 'update':
				case 'modify':
					modify(client, cmd);
					break;
				case 'remove':
					remove(client, cmd);
					break;
				default:
					cmd.builder.$callback(new Error('Operation "' + cmd.type + '" not found'));
					done();
					break;
			}
		}
	});
};

function WHERE(builder, scalar, group) {

	var condition = [];
	var sort = [];
	var tmp;

	for (var i = 0; i < builder.$commands.length; i++) {
		var cmd = builder.$commands[i];

		switch (cmd.type) {
			case 'where':
				tmp = ESCAPE(cmd.value);
				condition.push(SCOL + cmd.name + SCOL + (tmp == null && cmd.compare === '=' ? ' IS ' : cmd.compare) + tmp);
				break;
			case 'in':
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				if (cmd.value instanceof Array) {
					tmp = [];
					for (var j = 0; j < cmd.value.length; j++)
						tmp.push(ESCAPE(cmd.value[j]));
					condition.push(SCOL + cmd.name + SCOL + ' IN (' + tmp.join(',') + ')');
				} else
					condition.push(SCOL + cmd.name + SCOL + '=' + ESCAPE(cmd.value));
				break;
			case 'notin':
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				if (cmd.value instanceof Array) {
					tmp = [];
					for (var j = 0; j < cmd.value.length; j++)
						tmp.push(ESCAPE(cmd.value[j]));
					condition.push(SCOL + cmd.name + SCOL + ' NOT IN (' + tmp.join(',') + ')');
				} else
					condition.push(SCOL + cmd.name + SCOL + '<>' + ESCAPE(cmd.value));
				break;
			case 'between':
				condition.push('("' + cmd.name + SCOL + cmd.compare + '>=' + ESCAPE(cmd.a) + ' AND "' + cmd.name + '"<=' + ESCAPE(cmd.b));
				break;
			case 'search':
				tmp = ESCAPE((!cmd.compare || cmd.compare === '*' ? ('%' + cmd.value + '%') : (cmd.compare === 'beg' ? ('%' + cmd.value) : (cmd.value + '%'))));
				condition.push(SCOL + cmd.name + SCOL + ' LIKE ' + tmp);
				break;
			case 'fulltext':
				tmp = ESCAPE('%' + cmd.value.toLowerCase() + '%');
				condition.push('LOWER("' + cmd.name + '") LIKE ' + tmp);
				break;
			case 'contains':
				condition.push('LENGTH("' + cmd.name + +'"::text)>0');
				break;
			case 'empty':
				condition.push('("' + cmd.name + '" IS NULL OR LENGTH("' + cmd.name + +'"::text)=0)');
				break;
			case 'or':
				condition.push('(');
				break;
			case 'end':
				condition.push(')');
				break;
			case 'and':
				break;
			case 'sort':
				sort.push(SCOL + cmd.name + SCOL + ' ' + (cmd.desc ? 'DESC' : 'ASC'));
				break;
			case 'regexp':
				tmp = cmd.value.toString().substring(1);
				var g = '~';
				if (tmp[tmp.length - 1] === 'i') {
					tmp = tmp.substring(0, tmp.length - 2);
					g = '~*';
				} else
					tmp = tmp.substring(0, tmp.length - 1);
				condition.push(SCOL + cmd.name + SCOL + g + '\'' + tmp + '\'');
				break;
		}
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
	if (fields) {
		for (var i = 0; i < fields.length; i++)
			output += (output ? ',' : '') + fields[i];
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