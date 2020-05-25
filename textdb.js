const EMPTYARRAY = [];
const BLACKLIST = { dbms: 1 };
const FLAGS = ['json', 'post', 'keepalive'];
const ISOP = { '+': 1, '-': 1, '*': 1, '/': 1, '=': 1, '!': 1 };

function select(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder, null, null);
	var fields = FIELDS(builder);

	// opt.table
	var data = {};
	data.command = 'find';
	data.builder = {};

	if (fields)
		data.builder.fields = fields;

	data.builder.filter = filter.filter;
	data.builder.filterarg = { arg: filter.arg };

	if (filter.sort)
		filter.builder.sort = filter.sort;

	if (filter.take)
		data.builder.take = filter.take;

	if (filter.skip)
		data.builder.skip = filter.skip;

	F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);
	// builder.db.$debug && builder.db.$debug(q);

	data.db = opt.table;
	client.$opt.ws.senddata(data, function(err, response) {

		builder.db.busy = false;

		var rows = EMPTYARRAY;
		if (response instanceof Array) {
			err = response[0].error;
			response = null;
		} else if (response)
			rows = response.response;

		err && client.$opt.onerror && client.$opt.onerror(err, data);

		if (opt.first)
			rows = rows[0] || null;

		// checks joins
		if (!err && builder.$joins) {
			client.$dbms._joins(rows, builder, response ? response.count : 0);
			setImmediate(builder.db.$next);
		} else {
			builder.$callback(err, rows, response ? response.count : 0);
		}

	});
}

function check(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);
	var data = {};

	data.command = 'find2';
	data.builder = {};
	data.builder.filter = filter.filter;
	data.builder.filterarg = { arg: filter.arg };
	data.builder.take = 1;

	if (!cmd.value && builder.options.params)
		cmd.value = [];

	builder.db.$debug && builder.db.$debug(data);
	F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);
	data.db = opt.table;

	client.$opt.ws.senddata(data, function(err, response) {
		builder.db.busy = false;
		var is = false;
		if (response instanceof Array) {
			err = response[0].error;
			response = null;
		} else if (response && response.response.length)
			is = true;
		err && client.$opt.onerror && client.$opt.onerror(err, data);
		builder.$callback(err, is, response ? response.scanned : 0);
	});
}

function query(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);
	var data = {};

	data.command = 'find';
	data.builder = {};
	data.builder.filter = cmd.query + (filter.filter ? ('&&' + filter.filter) : '');
	data.builder.filterarg = cmd.value || {};
	data.builder.filterarg.arg = filter.arg;

	if (!cmd.value && builder.options.params)
		cmd.value = [];

	builder.db.$debug && builder.db.$debug(data);
	F.$events.dbms && EMIT('dbms', 'query', opt.table, opt.db);

	data.db = opt.table;

	client.$opt.ws.senddata(data, function(err, response) {
		builder.db.busy = false;

		if (response instanceof Array) {
			err = response[0].error;
			response = null;
		}

		err && client.$opt.onerror && client.$opt.onerror(err, data);
		builder.$callback(err, response ? response.response : EMPTYARRAY, response);
	});
}

function scalar(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var scalar = '';
	var data = {};
	var filter = WHERE(builder);

	data.command = 'find';
	data.builder = {};
	data.builder.filter = filter.filter;
	data.builder.filterarg = { arg: filter.arg };

	switch (cmd.scalar) {
		case 'avg':
			scalar = 'arg.value=(arg.value||0)+doc.' + cmd.name;
			break;
		case 'min':
			scalar = 'if (arg.value==null||doc.' + cmd.name + '<arg.value)arg.value=doc.' + cmd.name;
			break;
		case 'sum':
			scalar = 'arg.value=(arg.value||0)+doc.' + cmd.name;
			break;
		case 'max':
			scalar = 'if (arg.value==null||doc.' + cmd.name + '>arg.value)arg.value=doc.' + cmd.name;
			break;
		case 'count':
			scalar = 'arg.value=(arg.value||0)+1';
			break;
		case 'group':
			// @TODO: missing
			// scalar = 'if(!arg.value)arg.value={};if(arg.value[{0}])'
			break;
	}

	data.builder.scalar = scalar;
	data.builder.scalararg = {};
	data.db = opt.table;

	builder.db.$debug && builder.db.$debug(data);
	F.$events.dbms && EMIT('dbms', 'select', opt.table, opt.db);

	client.$opt.ws.senddata(data, function(err, response) {
		builder.db.busy = false;

		var value;

		if (response instanceof Array) {
			err = response[0].error;
			response = null;
		} else if (response) {
			value = response.scalararg.value;
			if (cmd.scalar === 'avg')
				value = (value / response.counter).fixed(3);
		}

		err && client.$opt.onerror && client.$opt.onerror(err, data);
		builder.$callback(err, value, response ? response.scanned : 0);
	});
}

function insert(client, cmd) {

	var builder = cmd.builder;

	cmd.builder.options.transform && cmd.builder.options.transform(cmd.builder.value, cmd.builder.db.$output, cmd.builder.db.$lastoutput);

	var keys = Object.keys(builder.value);
	var opt = builder.options;
	var doc = {};

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

		doc[key] = val == null ? null : typeof(val) === 'function' ? val(builder.value) : val;
	}

	// builder.db.$debug && builder.db.$debug(q);
	F.$events.dbms && EMIT('dbms', 'insert', opt.table, opt.db);

	var data = {};
	data.command = 'insert';
	data.builder = {};
	data.builder.payload = doc;
	data.db = opt.table;
	client.$opt.ws.senddata(data, function(err, response) {
		builder.db.busy = false;

		if (response instanceof Array) {
			err = response[0].error;
			response = null;
		}

		err && client.$opt.onerror && client.$opt.onerror(err, data);
		builder.$callback(err, err == null ? response.count : 0);
	});
}

function insertexists(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(cmd.builder);
	var data = {};

	data.command = 'find2';
	data.builder = {};
	data.builder.fields = 'none';
	data.builder.take = 1;
	data.builder.filter = filter.filter;
	data.builder.filterarg = { arg: filter.arg };

	F.$events.dbms && EMIT('dbms', 'select', opt.table, data);
	data.db = opt.table;
	client.$opt.ws.senddata(data, function(err, response) {
		builder.db.busy = false;

		var rows = EMPTYARRAY;

		if (response instanceof Array) {
			err = response[0].error;
			response = null;
		}

		err && client.$opt.onerror && client.$opt.onerror(err, data);
		var rows = response ? response.response : EMPTYARRAY;
		if (rows.length)
			builder.$callback(err, 0);
		else
			insert(client, cmd);
	});

}

function modify(client, cmd) {

	cmd.builder.options.transform && cmd.builder.options.transform(cmd.builder.value, cmd.builder.db.$output, cmd.builder.db.$lastoutput);

	var keys = Object.keys(cmd.builder.value);
	var params = [];
	var arr = [];
	var builder = [];
	var tmp;
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

		if (typeof(val) === 'function')
			val = val(cmd.builder.value);

		switch (c) {
			case '-':
			case '+':
			case '*':
			case '/':
				key = key.substring(1);
				params.push(val ? val : 0);
				builder.push('doc.' + key + '=(doc.' + key + '||0)' + c + push(arr, val ? val : 0));
				break;
			case '>':
			case '<':
				tmp = push(arr, val ? val : 0);
				key = key.substring(1);
				builder.push('doc.' + key + '=(doc.' + key + '||0)' + c + tmp + '?(doc.' + key + '||0):' + tmp);
				break;
			case '!':
				// toggle
				key = key.substring(1);
				builder.push('doc.' + key + '=!doc.' + key);
				break;
			case '=':
				// raw
				builder.push('doc.' + key + '=' + push(arr, val));
				break;
			default:
				builder.push('doc.' + key + '=' + push(arr, val));
				break;
		}
	}

	var opt = cmd.builder.options;

	if (opt.equal) {
		for (var i = 0; i < opt.equal.length; i++)
			cmd.builder.where(opt.equal[i], builder.value[opt.equal[i]]);
	}

	var filter = WHERE(cmd.builder);
	var data = {};

	data.command = 'update';
	data.builder = {};
	data.builder.filter = filter.filter;
	data.builder.filterarg = { arg: filter.arg };
	data.builder.modify = builder.join(';');
	data.builder.modifyarg = { arg: arr };

	if (filter.take)
		data.builder.take = filter.take;

	if (filter.skip)
		data.builder.skip = filter.skip;

	cmd.builder.db.$debug && cmd.builder.db.$debug(data);
	F.$events.dbms && EMIT('dbms', 'update', data);

	data.db = opt.table;
	client.$opt.ws.senddata(data, function(err, response) {

		cmd.builder.db.busy = false;

		var count = 0;

		if (response instanceof Array) {
			err = response[0].error;
			response = null;
		} else if (response)
			count = response.count;

		err && client.$opt.onerror && client.$opt.onerror(err, data);
		if (!count && cmd.insert) {
			if (cmd.insert !== true)
				cmd.builder.value = cmd.insert;
			cmd.builder.options.insert && cmd.builder.options.insert(cmd.builder.value, cmd.builder.options.insertparams);
			insert(client, cmd);
		} else
			cmd.builder.$callback(err, count, response ? response.scanned : 0);

	});
}

function remove(client, cmd) {
	var builder = cmd.builder;
	var opt = cmd.builder.options;
	var filter = WHERE(builder);
	var data = {};
	data.command = 'remove';
	data.builder = {};
	data.builder.filter = filter.filter;
	data.builder.filterarg = { arg: filter.arg };

	if (filter.take)
		data.builder.take = filter.take;

	if (filter.skip)
		data.builder.skip = filter.skip;

	builder.db.$debug && builder.db.$debug(data);
	F.$events.dbms && EMIT('dbms', 'delete', opt.table, opt.db);


	data.db = opt.table;
	client.$opt.ws.senddata(data, function(err, response) {

		cmd.builder.db.busy = false;

		var count = 0;

		if (response instanceof Array) {
			err = response[0].error;
			response = null;
		} else if (response)
			count = response.count;

		err && client.$opt.onerror && client.$opt.onerror(err, data);
		cmd.builder.$callback(err, count, response ? response.scanned : 0);

	});
}

function clientcommand(cmd, client) {
	switch (cmd.type) {
		case 'transaction':
		case 'end':
		case 'commit':
		case 'rollback':
			break;
		case 'find':
		case 'read':
		case 'list':
			select(client, cmd);
			break;
		case 'check':
			check(client, cmd);
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

exports.run = function(opt, self, cmd) {

	self.$op = null;
	self.busy = true;
	self.$opt = opt;

	if (!opt.is) {
		// socket
		opt.is = true;
		WEBSOCKETCLIENT(function(client) {

			var autocloseid;

			client.connect(opt.url);
			client.on('open', function() {

				opt.ws = client;
				client.callbacks = {};
				client.msgcounter = 1;
				client.pending = 0;

				client.senddata = function(data, callback) {

					autocloseid && clearTimeout(autocloseid);

					data.id = client.msgcounter++;
					client.pending++;

					if (callback)
						client.callbacks[data.id] = callback;

					client.send(data);
				};

				clientcommand(cmd, self);
			});

			client.on('error', function(e) {
				var err = 'TextDB connection error: ' + e;
				opt.onerror && opt.onerror(err);
			});

			client.on('close', function(e) {

				var err;

				if (e) {
					err = 'TextDB connection error: ' + e;
					opt.onerror && opt.onerror(err);
				}

				if (client.callbacks) {
					var keys = Object.keys(client.callbacks);
					for (var i = 0; i < keys.length; i++) {
						var cb = client.callbacks[keys[i]];
						cb && cb(err);
					}

					client.callbacks = null;
				}
				opt.is = false;
				opt.ws = null;
			});

			var closeforce = function() {
				client.close();
			};

			client.on('message', function(message) {
				client.pending--;
				var cb = client.callbacks[message.id];
				if (cb) {
					cb(message.err, message.response);
					delete client.callbacks[message.id];
				}

				if (!opt.pooling && !client.pending) {
					autocloseid && clearTimeout(autocloseid);
					autocloseid = setTimeout(closeforce, 100);
				}
			});

		});
	} else {
		if (opt.ws)
			clientcommand(cmd, self);
		else
			setTimeout(exports.run, 500, opt, self, cmd);
	}
};

function push(arr, value) {
	return 'arg.arg[' + (arr.push(value) - 1) + ']';
}

function WHERE(builder) {

	var condition = [];
	var sort = '';
	var tmp;
	var op = 'AND';
	var opuse = false;
	var arg = [];

	for (var i = 0; i < builder.$commands.length; i++) {
		var cmd = builder.$commands[i];

		if (builder.options.islanguage && cmd.name && cmd.name[cmd.name.length - 1] === '§')
			cmd.name = cmd.name.substring(0, cmd.name.length - 1) + (builder.options.language || '');

		switch (cmd.type) {
			case 'where':
				opuse && condition.length && condition.push(op);
				if (cmd.compare === '<>')
					cmd.compare = '!=';
				else if (cmd.compare === '=')
					cmd.compare = '==';
				condition.push('doc.' + cmd.name + cmd.compare + push(arg, cmd.value));
				break;
			case 'custom':
				cmd.fn.call(builder, builder, builder.db.$output, builder.db.$lastoutput);
				break;
			case 'in':
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				if (cmd.value instanceof Array) {
					opuse && condition.length && condition.push(op);
					condition.push(push(arg, cmd.value) + '.indexOf(doc.' + cmd.name + ')!==-1');
				} else {
					opuse && condition.length && condition.push(op);
					condition.push('doc.' + cmd.name + '==' + push(arg, cmd.value));
				}
				break;
			case 'notin':
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				if (cmd.value instanceof Array) {
					opuse && condition.length && condition.push(op);
					condition.push(push(arg, cmd.value) + '.indexOf(doc.' + cmd.name + ')===-1');
				} else {
					opuse && condition.length && condition.push(op);
					condition.push('doc.' + cmd.name + '!=' + push(arg, cmd.value));
				}
				break;
			case 'between':
				opuse && condition.length && condition.push(op);
				condition.push('(doc.' + cmd.name + '>=' + push(arg, cmd.a) + '&&doc.' + cmd.name + '<=' + push(arg, cmd.b) + ')');
				break;
			case 'search':
				// tmp = ESCAPE((!cmd.compare || cmd.compare === '*' ? ('%' + cmd.value + '%') : (cmd.compare === 'beg' ? ('%' + cmd.value) : (cmd.value + '%'))));
				opuse && condition.length && condition.push(op);
				condition.push('doc.' + cmd.name + '.indexOf(' + push(arg, tmp) + ')!==-1');
				break;

			case 'searchfull':
				// tmp = ESCAPE('%' + cmd.value.toLowerCase().replace(/y/g, 'i') + '%');
				// opuse && condition.length && condition.push(op);
				// condition.push('REPLACE(LOWER(to_tsvector(' + builder.options.table + '::text)::text), \'y\', \'i\') ILIKE ' + tmp);
				break;

			case 'searchall':
				// tmp = '';
				// for (var j = 0; j < cmd.value.length; j++)
				// 	tmp += (tmp ? ' AND ' : '') + cmd.name + ' ILIKE ' + ESCAPE('%' + cmd.value[j] + '%');
				// opuse && condition.length && condition.push(op);
				// condition.push('(' + (tmp || '0=1') + ')');
				break;

			case 'fulltext':
				// tmp = ESCAPE('%' + cmd.value.toLowerCase() + '%');
				// opuse && condition.length && condition.push(op);
				// condition.push('LOWER(' + cmd.name + ') ILIKE ' + tmp);
				break;
			case 'contains':
				opuse && condition.length && condition.push(op);
				condition.push('!!doc.' + cmd.name);
				break;
			case 'query':
				opuse && condition.length && condition.push(op);
				// condition.push('(' + (current == undefined ? cmd.query : cmd.query.replace(REG_PARAMS, replace)) + ')');
				break;
			case 'permit':
				break;
			case 'empty':
				opuse && condition.length && condition.push(op);
				condition.push('!doc.' + cmd.name);
				break;
			case 'month':
			case 'year':
			case 'day':
			case 'hour':
			case 'minute':
				// opuse && condition.length && condition.push(op);
				// condition.push('EXTRACT(' + cmd.type + ' from ' + cmd.name + ')' + cmd.compare + ESCAPE(cmd.value));
				break;
			case 'or':
				opuse && condition.length && condition.push(op);
				op = '||';
				opuse = false;
				condition.push('(');
				continue;
			case 'end':
				condition.push(')');
				op = '&&';
				break;
			case 'and':
				opuse && condition.length && condition.push(op);
				op = '&&';
				break;
			case 'sort':
				sort = cmd.name + '_' + (cmd.desc ? 'desc' : 'asc');
				break;
			case 'regexp':
				// tmp = cmd.value.toString().substring(1);
				// var g = '~';
				// if (tmp[tmp.length - 1] === 'i') {
				// 	tmp = tmp.substring(0, tmp.length - 2);
				// 	g = '~*';
				// } else
				// 	tmp = tmp.substring(0, tmp.length - 1);
				// opuse && condition.length && condition.push(op);
				// condition.push(cmd.name + g + '\'' + tmp + '\'');
				break;
		}
		opuse = true;
	}

	// var query = (condition.length ? (' WHERE ' + condition.join(' ')) : '') + (group ? (' GROUP BY ' + group) : '');
	return { filter: condition.length ? condition.join('') : 'true', arg: arg, sort: sort, take: builder.options.take, skip: builder.options.skip };
}

function FIELDS(builder) {
	var fields = builder.options.fields || '';
	return fields + (fields && fields.length && builder.$joinmeta ? (',' + builder.$joinmeta.a) : '');
}
