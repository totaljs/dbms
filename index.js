const Fs = require('fs');
const Url = require('url');
const Qs = require('querystring');
const CONN = {};
const CACHE = {};
const COMPARE = { '<': '<', '>': '>', '>=': '>=', '=>': '>=', '=<': '<=', '<=': '<=', '==': '=', '===': '=', '!=': '!=', '<>': '!=' };
const MODIFY = { insert: 1, update: 1, modify: 1 };

function promise(fn) {
	var self = this;
	return new Promise(function(resolve, reject) {
		self.callback(function(err, result) {
			if (err)
				reject(err);
			else
				resolve(fn == null ? result : fn(result));
		});
	});
}

function DBMS(ebuilder) {
	var self = this;
	self.$commands = [];
	self.$output = {};
	self.$outputall = {};
	self.$eb = global.ErrorBuilder != null;
	self.$errors = ebuilder || (global.ErrorBuilder ? new global.ErrorBuilder() : []);

	// self.$log;
	// self.$lastoutput;

	self.$next = function() {
		self.next();
	};
}

// DBMS.prototype = {
// 	get counter() {
// 		return this.$counter ? this.$counter : (this.$counter = new Counter(this));
// 	}
// };

const DP = DBMS.prototype;

DP.output = function(val) {
	this.$output = val;
	return this;
};

DP.debug = function() {
	this.$debug = function(val) {
		console.log('DBMS --->', val);
	};
	return this;
};

DP.callback = function(fn) {
	var self = this;
	self.$callback = fn;
	return self;
};

DP.data = function(fn) {
	var self = this;
	self.$callbackok = fn;
	return self;
};

DP.fail = function(fn) {
	var self = this;
	self.$callbackno = fn;
	return self;
};

DP.get = function(path) {
	var self = this;
	path = path.split('.');
	return function() {
		var data = self.$outputall;
		var p, tmp;
		for (var i = 0; i < path.length - 1; i++) {
			p = path[i];
			if (data && data[p] != null) {
				data = data[p];
			} else {
				data = null;
				break;
			}
		}
		p = path[path.length - 1];
		if (data instanceof Array) {
			tmp = [];
			for (var i = 0; i < data.length; i++) {
				var val = data[i][p];
				if (val != null)
					tmp.push(val);
			}
			return tmp;
		} else
			return data ? data[p] : null;
	};
};

DP.next = function() {
	var self = this;
	var cmd = self.$commands.shift();
	if (cmd) {
		if (cmd.type === 'task') {
			cmd.value.call(self, self.$outputall, self.$lastoutput);
			setImmediate(self.$next);
		} else if (cmd.type === 'validate') {
			var type = typeof(cmd.value);
			var stop = false;
			switch (type) {
				case 'function':
					var val = cmd.value(self.$lastoutput, self.$output);
					if (typeof(val) === 'string') {
						stop = true;
						self.$errors.push(val);
					}
					break;
				case 'string':
					if (self.$lastoutput instanceof Array) {
						if (cmd.reverse) {
							if (self.$lastoutput.length) {
								self.$errors.push(cmd.value);
								stop = true;
							}
						} else {
							if (!self.$lastoutput.length) {
								self.$errors.push(cmd.value);
								stop = true;
							}
						}
					} else {
						if (cmd.reverse) {
							if (self.$lastoutput) {
								self.$errors.push(cmd.value);
								stop = true;
							}
						} else {
							if (!self.$lastoutput) {
								self.$errors.push(cmd.value);
								stop = true;
							}
						}
					}
					break;
			}
			if (stop) {
				self.$commands = null;
				self.$callback(self.$errors, null);
			} else
				setImmediate(self.$next);
		} else {
			if (MODIFY[cmd.type] && cmd.value && typeof(cmd.value.$clean) === 'function')
				cmd.value = cmd.value.$clean();
			var conn = CONN[cmd.builder.options.db];
			require('./' + conn.db).run(conn, self, cmd);
		}
	} else {
		var err = self.$eb ? self.$errors.items.length > 0 ? self.$errors : null : self.$errors.length > 0 ? self.$errors : null;
		self.$callback && self.$callback(err, self.$output);
		if (err)
			self.$callbackno && self.$callbackno(err);
		else
			self.$callbackok && self.$callbackok(self.$output);

	}
	return self;
};

DP.make = function(fn) {
	var self = this;
	fn.call(self, self);
	return self;
};

DP.find = function(table) {
	var self = this;
	var builder = new QueryBuilder(self, 'find');
	builder.table(table);
	self.$commands.push({ type: 'find', builder: builder });
	self.$op && clearImmediate(self.$op);
	self.$op = setImmediate(self.$next);
	return builder;
};

DP.task = function(fn) {
	this.$commands.push({ type: 'task', value: fn });
	return this;
};

DP.list = DP.listing = function(table) {
	var self = this;
	var builder = new QueryBuilder(self, 'list');
	builder.table(table);
	builder.take(100);
	self.$commands.push({ type: 'list', builder: builder });
	self.$op && clearImmediate(self.$op);
	self.$op = setImmediate(self.$next);
	return builder;
};

DP.read = DP.one = function(table) {
	var self = this;
	var builder = new QueryBuilder(self, 'find');
	builder.table(table);
	builder.first();
	self.$commands.push({ type: 'find', builder: builder });
	self.$op && clearImmediate(self.$op);
	self.$op = setImmediate(self.$next);
	return builder;
};

DP.scalar = function(table, type, name) {

	// type: avg
	// type: count
	// type: group
	// type: max
	// type: min
	// type: sum

	var self = this;
	var builder = new QueryBuilder(self, 'scalar');
	builder.table(table);
	self.$commands.push({ type: 'scalar', builder: builder, scalar: type, name: name });
	self.$op && clearImmediate(self.$op);
	self.$op = setImmediate(self.$next);
	return builder;
};

DP.count = function(table) {
	return this.scalar(table, 'count');
};

DP.max = function(table, prop) {
	return this.scalar(table, 'max', prop);
};

DP.min = function(table, prop) {
	return this.scalar(table, 'min', prop);
};

DP.avg = function(table, prop) {
	return this.scalar(table, 'avg', prop);
};

DP.sum = function(table, prop) {
	return this.scalar(table, 'sum', prop);
};

DP.group = function(table, prop) {
	return this.scalar(table, 'group', prop);
};

DP.insert = function(table, value, unique) {
	var self = this;
	var builder = new QueryBuilder(self, 'insert');
	builder.table(table);
	builder.first();
	self.$commands.push({ type: 'insert', builder: builder, value: value, unique: unique });
	self.$op && clearImmediate(self.$op);
	self.$op = setImmediate(self.$next);
	return builder;
};

DP.update = function(table, value, insert) {
	var self = this;
	var builder = new QueryBuilder(self, 'update');
	builder.table(table);
	self.$commands.push({ type: 'update', builder: builder, value: value, insert: insert });
	self.$op && clearImmediate(self.$op);
	self.$op = setImmediate(self.$next);
	return builder;
};

DP.modify = function(table, value, insert) {
	var self = this;
	var builder = new QueryBuilder(self, 'modify');
	builder.table(table);
	self.$commands.push({ type: 'modify', builder: builder, value: value, insert: insert });
	self.$op && clearImmediate(self.$op);
	self.$op = setImmediate(self.$next);
	return builder;
};

DP.remove = function(table) {
	var self = this;
	var builder = new QueryBuilder(self, 'remove');
	builder.table(table);
	self.$commands.push({ type: 'remove', builder: builder });
	self.$op && clearImmediate(self.$op);
	self.$op = setImmediate(self.$next);
	return builder;
};

DP.must = DP.validate = function(err, reverse) {
	var self = this;
	self.$commands.push({ type: 'validate', value: err || 'unhandled exception', reverse: reverse });
	return self;
};

function QueryBuilder(db, type) {
	var self = this;
	self.db = db;
	self.$commands = [];
	self.options = { db: 'default', type: type, take: 0, skip: 0, first: false, fields: null, dynamic: false };
}

const QB = QueryBuilder.prototype;
const NOOP = function(){};

QB.promise = promise;

QB.log = function(msg, user) {
	var self = this;
	if (msg) {
		NOW = new Date();
		self.$log = (self.$log ? self.$log : '') + NOW.format('yyyy-MM-dd HH:mm:ss') + ' | '  + self.options.table.padRight(25) + ': ' + (user ? '[' + user.padRight(20) + '] ' : '') + msg + '\n';
	} else if (self.$log) {
		Fs.appendFile(F.path.logs('dmbs.log'), self.$log, NOOP);
		self.$log = null;
	}
	return self;
};

QB.table = function(table) {

	var self = this;
	var cache = CACHE[table];

	if (!cache) {
		var tmp = table.split('/');
		cache = { db: tmp.length > 1 ? tmp[0] : 'default', table: tmp.length > 1 ? tmp[1] : tmp[0] };
		CACHE[table] = cache;
	}

	self.options.db = cache.db;
	self.options.table = cache.table;
	return self;
};

QB.$callback = function(err, value, count) {

	var self = this;
	var opt = self.options;

	self.$log && self.log();

	if (opt.type === 'list') {
		value = { items: value, count: count };
		value.page = (opt.skip / opt.take) + 1;
		value.limit = opt.take;
		value.pages = Math.ceil(count / value.limit);
	}

	opt.callback && opt.callback(err, value, count);

	if (err) {

		self.db.$errors.push(err);
		self.db.$lastoutput = null;
		self.db.$outputall[opt.table] = null;
		opt.callbackno && opt.callbackno(err);

	} else {

		self.db.$outputall[opt.table] = self.db.$lastoutput = value;
		if (opt.assign)
			self.db.$outputall[opt.assign] = self.db.$output[opt.assign] = value;
		else
			self.db.$output = value;

		var ok = true;
		if (opt.validate) {
			if (value instanceof Array) {
				if (!value.length) {
					self.db.$errors.push(opt.validate);
					ok = false;
				}
			} else if (!value) {
				self.$errors.push(opt.validate);
				ok = false;
			}
		}

		if (ok)
			opt.callbackok && opt.callbackok(value, count);
		else
			opt.callbackno && opt.callbackno(opt.validate);
	}

	setImmediate(self.db.$next);
};

QB.make = function(fn) {
	var self = this;
	fn.call(self, self);
	return self.db;
};

QB.set = QB.assign = function(prop) {
	var self = this;
	self.options.assign = prop == null ? '' : prop;
	return self;
};

QB.where = function(name, compare, value) {

	if (value === undefined) {
		value = compare;
		compare = '=';
	} else {
		compare = COMPARE[compare];
		if (compare == null)
			throw new Error('DBMS: comparer "' + compare + '" is not supported for QueryBuilder.');
	}

	var self = this;
	self.$commands.push({ type: 'where', name: name, value: value, compare: compare });
	return self;
};

QB.in = function(name, value) {
	var self = this;
	self.$commands.push({ type: 'in', name: name, value: value });
	return self;
};

QB.notin = function(name, value) {
	var self = this;
	self.$commands.push({ type: 'notin', name: name, value: value });
	return self;
};

QB.between = function(name, a, b) {
	var self = this;
	self.$commands.push({ type: 'between', name: name, a: a, b: b });
	return self;
};

QB.search = function(name, value, compare) {
	var self = this;
	self.$commands.push({ type: 'search', name: name, value: value, compare: compare == null ? '*' : compare });
	return self;
};

QB.fulltext = function(name, value, weight) {
	var self = this;
	self.$commands.push({ type: 'fulltext', name: name, value: value, weight: weight });
	return self;
};

QB.regexp = function(name, value) {
	var self = this;
	self.$commands.push({ type: 'regexp', name: name, value: value });
	return self;
};

QB.contains = function(name) {
	var self = this;
	self.$commands.push({ type: 'contains', name: name });
	return self;
};

QB.empty = function(name) {
	var self = this;
	self.$commands.push({ type: 'empty', name: name });
	return self;
};

QB.first = function() {
	var self = this;
	self.options.first = true;
	self.take(1);
	return self;
};

QB.sort = function(name, desc) {
	var self = this;
	self.$commands.push({ type: 'sort', name: name, desc: desc == true || desc === 'desc' });
	return self;
};

QB.take = function(value) {
	var self = this;
	self.options.take = value;
	return self;
};

QB.skip = function(value) {
	var self = this;
	self.options.skip = value;
	return self;
};

QB.limit = function(value) {
	var self = this;
	self.options.limit = value;
	return self;
};

QB.page = function(page, limit) {
	var self = this;
	if (limit)
		self.options.take = limit;
	self.options.take = page * self.options.take;
	return self;
};

QB.paginate = function(page, limit, maxlimit) {

	var self = this;
	var limit2 = +(limit || 0);
	var page2 = (+(page || 0)) - 1;

	if (page2 < 0)
		page2 = 0;

	if (maxlimit && limit2 > maxlimit)
		limit2 = maxlimit;

	if (!limit2)
		limit2 = maxlimit;

	self.options.skip = page2 * limit2;
	self.options.take = limit2;
	return self;
};

QB.callback = function(callback) {
	var self = this;
	self.options.callback = callback;
	return self;
};

QB.data = function(fn) {
	var self = this;
	self.options.callbackok = fn;
	return self;
};

QB.fail = function(fn) {
	var self = this;
	self.options.callbackno = fn;
	return self;
};

QB.must = QB.validate = function(err) {
	var self = this;
	self.options.validate = err || 'unhandled exception';
	return self;
};

QB.insert = function(callback) {
	var self = this;
	self.options.insert = callback;
	return self;
};

QB.query = function(value) {
	var self = this;
	self.$commands.push({ type: 'query', value: value });
	return self;
};

QB.or = function(fn) {
	var self = this;
	self.$commands.push({ type: 'or' });
	fn();
	self.$commands.push({ type: 'end' });
	return self;
};

QB.fields = function() {
	var self = this;
	if (!self.options.fields)
		self.options.fields = [];
	for (var i = 0; i < arguments.length; i++)
		self.options.fields.push(arguments[i]);
	return self;
};

QB.year = function(name, compare, value) {

	if (value === undefined) {
		value = compare;
		compare = '=';
	} else {
		compare = COMPARE[compare];
		if (compare == null)
			throw new Error('DBMS: comparer "' + compare + '" is not supported for QueryBuilder.');
	}

	var self = this;
	self.$commands.push({ type: 'year', name: name, value: value, compare: compare });
	return self;
};

QB.month = function(name, compare, value) {

	if (value === undefined) {
		value = compare;
		compare = '=';
	} else {
		compare = COMPARE[compare];
		if (compare == null)
			throw new Error('DBMS: comparer "' + compare + '" is not supported for QueryBuilder.');
	}

	var self = this;
	self.$commands.push({ type: 'month', name: name, value: value, compare: compare });
	return self;
};

QB.day = function(name, compare, value) {

	if (value === undefined) {
		value = compare;
		compare = '=';
	} else {
		compare = COMPARE[compare];
		if (compare == null)
			throw new Error('DBMS: comparer "' + compare + '" is not supported for QueryBuilder.');
	}

	var self = this;
	self.$commands.push({ type: 'day', name: name, value: value, compare: compare });
	return self;
};

QB.hour = function(name, compare, value) {

	if (value === undefined) {
		value = compare;
		compare = '=';
	} else {
		compare = COMPARE[compare];
		if (compare == null)
			throw new Error('DBMS: comparer "' + compare + '" is not supported for QueryBuilder.');
	}

	var self = this;
	self.$commands.push({ type: 'hour', name: name, value: value, compare: compare });
	return self;
};

QB.minute = function(name, compare, value) {

	if (value === undefined) {
		value = compare;
		compare = '=';
	} else {
		compare = COMPARE[compare];
		if (compare == null)
			throw new Error('DBMS: comparer "' + compare + '" is not supported for QueryBuilder.');
	}

	var self = this;
	self.$commands.push({ type: 'minute', name: name, value: value, compare: compare });
	return self;
};

QB.query = function(value) {
	var self = this;
	self.$commands.push({ type: 'code', value: value });
	return self;
};

exports.QueryBuilder = QueryBuilder;
exports.DBMS = DBMS;
exports.make = function(fn) {
	var self = new DBMS();
	fn.call(self, self);
	return self;
};

exports.init = function(name, connection) {

	if (connection == null || typeof(connection) === 'function') {
		connection = name;
		name = 'default';
	}

	// Total.js
	if (connection === 'nosql' || connection === 'table') {
		CONN[name] = { id: name, db: 'total', type: connection };
		return exports;
	}

	var opt = Url.parse(connection);
	var q = Qs.parse(opt.query);
	var tmp = {};
	var arr;

	switch (opt.protocol) {
		case 'postgresql:':
		case 'postgres:':
		case 'postgre:':
		case 'pg:':
			if (opt.auth) {
				arr = opt.auth.split(':');
				tmp.user = arr[0] || '';
				tmp.password = arr[1] || '';
			}
			tmp.host = opt.hostname;
			tmp.port = opt.port;
			tmp.database = opt.pathname.split('/')[1];
			tmp.ssl = q.ssl === '1' || q.ssl === 'true' || q.ssl === 'on';
			tmp.max = +(q.max || '4');
			tmp.min = +(q.min || '2');
			tmp.idleTimeoutMillis = +(q.timeout || '1000');
			tmp.native = q.native === '1' || q.native === 'true' || q.native === 'on';
			CONN[name] = { id: name, db: 'pg', options: tmp };
			break;
	}

	return exports;
};

global.DBMS = function(err) {
	if (typeof(err) === 'function') {
		var db = new exports.DBMS();
		err.call(db, db);
	} else
		return new exports.DBMS(err);
};

// Total.js framework
if (global.F) {
	global.F.database = function(err) {
		if (typeof(err) === 'function') {
			var db = new exports.DBMS();
			err.call(db, db);
		} else
			return new exports.DBMS(err);
	};
}