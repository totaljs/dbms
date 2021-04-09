const Fs = require('fs');
const Url = require('url');
const Qs = require('querystring');
const CONN = {};
const CACHE = {};
const COMPARE = { '<': '<', '>': '>', '>=': '>=', '=>': '>=', '=<': '<=', '<=': '<=', '==': '=', '===': '=', '!=': '!=', '<>': '!=', '=': '=' };
const MODIFY = { insert: 1, update: 1, modify: 1 };
const TEMPLATES = {};
const REG_FIELDS_CLEANER = /"|`|\||'|\s/g;
const CACHEBLACKLIST = { insert: 1, modify: 1, update: 1, remove: 1 };

// A temporary cache for fields (it's cleaning each 10 minutes)
var FIELDS = {};
var auditwriter;

function promise(fn) {

	var self = this;
	var $;

	if (fn && typeof(fn) === 'object') {
		$ = fn;
		fn = null;
	}

	return new Promise(function(resolve, reject) {
		self.callback(function(err, result) {
			if (err) {
				if ($)
					$.invalid(err);
				else
					reject(err);
			} else
				resolve(fn ? fn(result) : result);
		});
	});
}

var logger;

function DBMS(errbuilder) {

	var self = this;
	self.$conn = {};
	self.$commands = [];
	self.$output = {};
	self.response = self.$outputall = {};
	self.$eb = global.ErrorBuilder != null;
	self.$errors = errbuilder || (global.ErrorBuilder ? new global.ErrorBuilder() : []);

	// self.$log;
	// self.$lastoutput;

	self.$next = function(err) {
		err && self.$errors.push(err);
		self.next();
	};
}

const DP = DBMS.prototype;

DP.promise = promise;

DP.cache = function(key, expire) {
	var self = this;
	var f = expire[0];
	if (f === 'c' || f === 'r') {
		exports.cache_set(key, expire);
	} else {
		self.$cachekey = key;
		self.$cacheexpire = expire;
	}
	return self;
};

DP.blob = function(table) {

	if (!table)
		table = 'default';

	var cache = CACHE[table];
	if (!cache) {
		var tmp = table.split('/');
		cache = { db: tmp.length > 1 ? tmp[0] : 'default', table: tmp.length > 1 ? tmp[1] : tmp[0] };
		CACHE[table] = cache;
	}

	var conn = CONN[cache.db];
	var driver = require('./' + conn.db);

	return {
		write: function(stream, filename, callback) {

			if (stream instanceof Buffer) {

				if (typeof(filename) === 'function') {
					callback = filename;
					filename = null;
				}

				// Creates a temporary file
				var tmpfile = PATH.temp(Math.random().toString(16).substring(3) + '.dbms');

				Fs.writeFile(tmpfile, stream, function(err) {

					if (err) {
						callback(err);
						return;
					}

					stream = Fs.createReadStream(tmpfile);
					driver.blob_write(conn, stream, filename, callback, cache);
					stream.on('close', () => Fs.unlink(tmpfile, NOOP));
				});

			} else
				driver.blob_write(conn, stream, filename, callback, cache);
		},
		read: function(id, callback) {
			driver.blob_read(conn, id, callback, cache);
		},
		remove: function(id, callback) {
			driver.blob_remove(conn, id, callback, cache);
		}
	};
};

DP.output = function(val) {
	this.$output = val;
	return this;
};

function debug(val) {
	console.log('DBMS --->', val);
}

DP.debug = function() {
	this.$debug = debug;
	return this;
};

DP.log = DP.audit = function() {
	var arg = [];
	var self = this;

	for (var i = 0; i < arguments.length; i++)
		arg.push(arguments[i]);

	self.$commands.push({ type: 'audit', arg: arg });

	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}

	return self;
};

DP.invalid = function(name, err) {
	var self = this;
	self.$errors.push(name, err);
	return self;
};

DP.kill = function(reason) {
	var self = this;
	self.$commands.length = 0;
	reason && self.$errors.push(reason);
	return self;
};

DP.done = function($, callback, param) {
	this.$callback = function(err, response) {
		if (err)
			$.invalid(err);
		else
			callback(response, param);
	};
	return this;
};

DP.callback = function(fn) {
	var self = this;

	if (typeof(fn) === 'function') {
		self.$callback = fn;
		return self;
	} else {
		self.$ = fn;
		return new Promise(function(resolve, reject) {
			self.$resolve = resolve;
			self.$reject = reject;
		});
	}

};

DP.data = function(fn, param) {
	var self = this;
	self.$callbackok = fn;
	self.$callbackokparam = param;
	return self;
};

DP.fail = function(fn, param) {
	var self = this;
	self.$callbackno = fn;
	self.$callbacknoparam = param;
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
	if (self.$skip)
		return;

	var cmd = self.$commands.shift();
	logger && loggerend(self);

	if (self.$op) {
		clearImmediate(self.$op);
		self.$op = null;
	}

	var stop = false;

	if (cmd) {

		if (cmd.builder && self.prev && self.prev.builder) {
			if (cmd.builder.$prevfilter) {
				for (var i = 0; i < self.prev.builder.$commands.length; i++)
					cmd.builder.$commands.push(self.prev.builder.$commands[i]);
			}
			if (cmd.builder.$prevfields && self.prev.builder.options.fields)
				cmd.builder.options.fields = self.prev.builder.options.fields;
		}

		if (cmd.builder && cmd.builder.$joinmeta) {
			if (!cmd.builder.$joinmeta.can) {
				self.$commands.push(cmd);
				setImmediate(self.$next);
				return;
			}
		}

		if (cmd.builder && cmd.builder.disabled) {
			setImmediate(self.$next);
			return;
		}

		if (cmd.type === 'audit') {
			auditwriter && auditwriter.apply(self, cmd.arg);
			self.$op && clearImmediate(self.$op);
			self.$op = setImmediate(self.$next);
		} else if (cmd.type === 'task') {
			cmd.value.call(self, self.$outputall, self.$lastoutput);
			if (self.$errors.length) {
				self.$commands = null;

				if (self.$) {
					self.$.invalid(self.$errors);
					self.$ = null;
				}

				if (self.$callback) {
					try {
						self.$callback(self.$errors, null);
						self.$callback = null;
					} catch (e) {
						self.unexpected(e);
					}
				}

				if (self.$callbackno) {
					try {
						self.$callbackno(self.$errors, self.$callbacknoparam);
						self.$callbacknoparam = self.$callbackno = null;
					} catch (e) {
						self.unexpected(e);
					}
				}

				self.forcekill();
			} else {
				self.$op && clearImmediate(self.$op);
				self.$op = setImmediate(self.$next);
			}
		} else if (cmd.type === 'validate') {

			if (cmd.value == null) {
				if (self.$lasterror)
					stop = true;
			} else {
				var type = typeof(cmd.value);
				switch (type) {
					case 'function':
						var val = cmd.value(self.$lastoutput, self.$output);
						if (typeof(val) === 'string') {
							stop = true;
							self.$errors.push(val);
						}
						break;
					case 'string':
					case 'number':

						if (type === 'number')
							cmd.value += '';

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
			}

			if (stop) {

				self.$commands = null;

				if (self.$) {
					self.$.invalid(self.$errors);
					self.$ = null;
				}

				if (self.$callback) {
					try {
						self.$callback(self.$errors, null);
						self.$callback = null;
					} catch (e) {
						self.unexpected(e);
					}
				}

				if (self.$callbackno) {
					try {
						self.$callbackno(self.$errors, self.$callbacknoparam);
						self.$callbacknoparam = self.$callbackno = null;
					} catch (e) {
						self.unexpected(e);
					}
				}

				self.forcekill();

			} else
				setImmediate(self.$next);
		} else {

			if (MODIFY[cmd.type] && cmd.value && typeof(cmd.value.$clean) === 'function')
				cmd.value = cmd.value.$clean();

			var conn = CONN[cmd.conn || cmd.builder.options.db];

			// Due to TextDB.query()
			if (!conn)
				conn = CONN.default;

			if (conn) {
				if (self.$cachekey) {
					exports.cache_get(self.$cachekey, cmd.builder.options.assign || 'default', function(err, cache) {
						if (cache) {
							cmd.builder.$callback(err, cache.response, cache.count, true);
						} else {
							logger && loggerbeg(self, cmd);
							require('./' + conn.db).run(conn, self, cmd);
						}
					});
				} else {
					logger && loggerbeg(self, cmd);
					require('./' + conn.db).run(conn, self, cmd);
				}

			} else {
				var err = new Error('Connection string "' + (cmd.conn || cmd.builder.options.db) + '" is not initialized.');
				if (cmd.builder)
					cmd.builder.$callback(err);
				else
					cmd.db.$next(err);
			}
		}

		self.prev = cmd;
	} else {

		self.forcekill();
		var err = self.$eb ? self.$errors.items.length > 0 ? self.$errors : null : self.$errors.length > 0 ? self.$errors : null;

		if (self.$) {
			if (err)
				self.$.invalid(err);
			else {
				try {
					self.$resolve(self.$output);
				} catch (e) {
					self.unexpected(e);
				}
			}
			self.$reject = self.$resolve = null;
			self.$ = null;
		}

		if (self.$callback) {
			try {
				self.$callback(err, self.$output);
				self.$callback = null;
			} catch (e) {
				self.unexpected(e);
			}
		}

		if (err) {
			if (self.$callbackno) {
				self.$callbackno(err, self.$callbacknoparam);
				self.$callbacknoparam = self.$callbackno = null;
			}
		} else {
			if (self.$callbackok) {
				self.$callbackok(self.$output, self.$callbackokparam);
				self.$callbackokparam = self.$callbackok = null;
			}
		}
	}

	return self;
};

DP.unexpected = function(e) {
	this.forcekill();
	throw e;
};

DP.forcekill = function() {
	var self = this;
	if (self.$conn) {
		self.closed = true;
		var keys = Object.keys(self.$conn);
		for (var i = 0; i < keys.length; i++) {
			var item = self.$conn[keys[i]];
			if (item) {
				item.$$destroy(item);
				self.$conn[keys[i]] = null;
			}
		}
	}
};

function loggerbeg(self, cmd) {
	cmd.ts = new Date();
	self.$logger = cmd;
}

function loggerend(self) {
	if (self.$logger) {
		var ln = (self.$logger.builder.options.db === 'default' ? '' : (self.$logger.builder.options.db + '/')) + (self.$logger.builder.options.table || '');
		NOW = new Date();
		logger(NOW.format('yyyy-MM-dd HH:mm:ss'), 'DBMS logger: ' + (ln ? (ln + '.') : '') + self.$logger.type + '()', self.$logger.builder.$count + 'x', ((NOW - self.$logger.ts) / 1000) + 's');
		self.$logger = null;
	}
}

DP.make = function(fn) {
	var self = this;
	fn.call(self, self);
	return self;
};

DP.all = DP.find = function(table) {
	var self = this;
	var builder = new QueryBuilder(self, 'find');
	builder.table(table);
	self.$commands.push({ type: 'find', builder: builder });
	if (!self.$joinmeta && !self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}
	return builder;
};

DP.dif = DP.diff = function(table, form, prop) {
	var self = this;
	var builder = new QueryBuilder(self, 'diff');
	builder.table(table);
	self.$commands.push({ type: 'diff', builder: builder, form: form, key: prop || 'id' });
	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}
	return builder;
};

DP.task = function(fn) {
	this.$commands.push({ type: 'task', value: fn });
	return this;
};

DP.list = DP.listing = function(table, improved) {
	var self = this;
	var builder = new QueryBuilder(self, 'list');
	builder.table(table);
	builder.options.take = 100;
	self.$commands.push({ type: 'list', builder: builder, improved: improved });
	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}
	return builder;
};

DP.read = DP.one = function(table) {
	var self = this;
	var builder = new QueryBuilder(self, 'read');
	builder.table(table);
	builder.options.first = true;
	builder.options.take = 1;
	self.$commands.push({ type: 'read', builder: builder });
	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}
	return builder;
};

DP.check = function(table) {
	var self = this;
	var builder = new QueryBuilder(self, 'check');
	builder.table(table);
	builder.options.first = true;
	builder.options.take = 1;
	self.$commands.push({ type: 'check', builder: builder });
	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}
	return builder;
};

DP.stream = function(table, limit, callback, done) {
	var self = this;
	var builder = new QueryBuilder(self, 'find');
	builder.table(table);
	builder.options.take = limit;
	builder.options.skip = 0;

	var count = 0;
	var page = 1;

	var cb = function(err, response) {

		if (!response || response.length === 0) {
			// done
			done && done(null, count);
			return;
		}

		callback(response, function(stop) {

			if (stop) {
				done && done(null, count);
				return;
			}

			builder.db.forcekill();
			builder.options.skip = limit * (page++);

			var db = new DBMS(builder.$errors);
			builder.db = db;
			db.$commands.push({ type: 'find', builder: builder });
			db.$op && clearImmediate(db.$op);
			db.$op = setImmediate(db.$next);
		});
	};

	builder.callback(cb);
	self.$commands.push({ type: 'find', builder: builder });

	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(function() {
			var is = false;
			for (var i = 0; i < builder.$commands.length; i++) {
				var cmd = builder.$commands[i];
				if (cmd.type === 'sort') {
					is = true;
					break;
				}
			}
			!is && builder.sort('1');
			self.$next();
		});
	}

	return builder;
};

DP.scalar = function(table, type, name, field) {

	// type: avg
	// type: count
	// type: group
	// type: max
	// type: min
	// type: sum

	var self = this;
	var builder = new QueryBuilder(self, 'scalar');
	builder.table(table);
	self.$commands.push({ type: 'scalar', builder: builder, scalar: type, name: name, field: field });
	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}
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

DP.begin = function(conn) {
	var self = this;
	self.$commands.push({ type: 'transaction', db: self, conn: conn || 'default' });
	return self;
};

DP.commit = function(conn) {
	var self = this;
	self.$commands.push({ type: 'commit', db: self, conn: conn || 'default' });
	return self;
};

DP.end = function(conn) {
	var self = this;
	self.$commands.push({ type: 'end', db: self, conn: conn || 'default' });
	return self;
};

DP.rollback = DP.abort = function(conn) {
	var self = this;
	self.$commands.push({ type: 'rollback', db: self, conn: conn || 'default' });
	return self;
};

DP.save = function(table, isUpdate, obj, fn) {

	if (obj == null || typeof(obj) === 'function') {
		fn = obj;
		obj = isUpdate;
		isUpdate = !!obj.id;
	}

	var builder = isUpdate ? this.modify(table, obj) : this.insert(table, obj);
	fn && fn.call(builder, builder, isUpdate, builder.value);
	return builder;
};

DP.add = DP.ins = DP.insert = function(table, value, unique) {
	var self = this;
	var builder = new QueryBuilder(self, 'insert');
	builder.table(table);
	builder.value = value || {};

	if (unique) {
		builder.options.first = true;
		builder.options.take = 1;
	}

	// Total.js schemas
	if (builder.value.$clean)
		builder.value = builder.value.$clean();

	builder.$commandindex = self.$commands.push({ type: 'insert', builder: builder, unique: unique }) - 1;

	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}

	return builder;
};

DP.upd = DP.update = function(table, value, insert) {
	var self = this;
	var builder = new QueryBuilder(self, 'update');
	builder.table(table);
	builder.value = value || {};

	// Total.js schemas
	if (builder.value.$clean)
		builder.value = builder.value.$clean();

	builder.$commandindex = self.$commands.push({ type: 'update', builder: builder, insert: insert }) - 1;

	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}

	return builder;
};

DP.mod = DP.modify = function(table, value, insert) {

	var self = this;
	var builder;

	if (typeof(value) === 'function') {
		builder = new QueryBuilder(self, 'read');
		builder.table(table);
		builder.options.first = true;
		builder.options.take = 1;
		builder.$commandindex = self.$commands.push({ type: 'modify2', builder: builder, fn: value, insert: insert }) - 1;
	} else {
		builder = new QueryBuilder(self, 'modify');
		builder.table(table);
		builder.value = value || {};
		// Total.js schemas
		if (builder.value.$clean)
			builder.value = builder.value.$clean();
		builder.$commandindex = self.$commands.push({ type: 'modify', builder: builder, insert: insert }) - 1;
	}

	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}

	return builder;
};

DP.que = DP.query = function(conn, query, value) {

	if (query == null || typeof(query) === 'object') {
		value = query;
		query = conn;
		conn = null;
	}

	var self = this;
	var builder = new QueryBuilder(self, 'query');
	builder.options.db = conn || 'default';
	self.$commands.push({ type: 'query', builder: builder, query: query, value: value });
	value && (builder.options.params = true);

	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}

	return builder;
};

DP.rem = DP.remove = function(table) {
	var self = this;
	var builder = new QueryBuilder(self, 'remove');
	builder.table(table);
	self.$commands.push({ type: 'remove', builder: builder });

	if (!self.busy) {
		self.$op && clearImmediate(self.$op);
		self.$op = setImmediate(self.$next);
	}

	return builder;
};

DP.err = DP.error = DP.must = DP.validate = function(err, reverse) {
	var self = this;
	self.$commands.push({ type: 'validate', value: err, reverse: reverse });
	return self;
};

function QueryBuilder(db, type) {
	var self = this;
	// cloning
	if (db instanceof QueryBuilder) {
		self.db = db.db;
		self.$ormprimary = db.$ormprimary;
		self.$ormprimaryremove = db.$ormprimaryremove;
		self.$primarykey = db.$primarykey;
		self.$commands = db.$commands.slice(0);
		self.options = clone(db.options);
	} else {
		self.db = db;
		self.$commands = [];
		self.options = { db: 'default', type: type, take: 0, skip: 0, first: false, fields: null, dynamic: false };
	}
}

function clone(obj) {
	var keys = Object.keys(obj);
	var o = {};
	for (var i = 0; i < keys.length; i++)
		o[keys[i]] = obj[keys[i]];
	return o;
}

const QB = QueryBuilder.prototype;
const NOOP = function(){};

QB.promise = promise;

QB.cache = function(key, expire) {
	var self = this;
	self.db.cache(key, expire);
	return self;
};

QB.primarykey = function(key) {
	var self = this;
	self.$primarykey = key;
	return self;
};

QB.prevfilter = function() {
	var self = this;
	self.$prevfilter = 1;
	return self;
};

QB.custom = function(fn) {
	this.$commands.push({ type: 'custom', fn: fn });
	return this;
};

QB.prevfields = function() {
	var self = this;
	self.$prevfields = 1;
	return self;
};

QB.use = function(name, arg) {
	if (TEMPLATES[name])
		TEMPLATES[name](this, arg);
	return this;
};

QB.get = function(path) {
	return this.db.get(path);
};

QB.log = function(msg, user) {
	var self = this;

	if (auditwriter) {
		self.db.log.apply(self.db, arguments);
		return self;
	}

	if (msg) {
		NOW = new Date();
		self.$log = (self.$log ? self.$log : '') + NOW.format('yyyy-MM-dd HH:mm:ss') + ' | '  + self.options.table.padRight(25) + ': ' + (user ? '[' + user.padRight(20) + '] ' : '') + msg + '\n';
	} else if (self.$log) {
		Fs.appendFile(F.path.logs('dbms.log'), self.$log, NOOP);
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
		cache.type = CONN[cache.db] ? CONN[cache.db].type : '';
		CACHE[table] = cache;
	}
	self.options.path = table;
	self.options.db = cache.db;
	self.options.table = cache.table;
	self.options.dbname = cache.type;
	return self;
};

QB.conn = function(name) {
	var self = this;
	self.options.db = name;
	return self;
};

QB.orm = function(primary) {
	var self = this;
	self.$orm = 1;
	self.$ormprimary = primary || '';
	if (primary && self.options.fields && self.options.fields.indexOf(primary) === -1) {
		self.options.fields.push(primary);
		self.$ormprimaryremove = 1;
	}
	return self;
};

QB.$callback = function(err, value, count, iscache) {

	var self = this;
	var opt = self.options;

	self.$log && self.log();

	if (logger)
		self.$count = value instanceof Array ? value.length : value != null ? 1 : 0;

	if (opt.type === 'list') {
		value = { items: value, count: count };
		value.page = (opt.skip / opt.take) + 1;
		value.limit = opt.take;
		value.pages = Math.ceil(count / value.limit);
	}

	if (value) {
		if (self.$orm) {
			self.$orm = 2;
			if (value instanceof Array) {
				for (var i = 0; i < value.length; i++) {
					if (opt.fieldsrem) {
						for (var j = 0; j < opt.fieldsrem.length; j++)
							value[i][opt.fieldsrem[j]] = undefined;
					}
					value[i].dbms = new QueryBuilder(self);
					value[i].dbms.value = value[i];
				}
			} else {
				if (opt.fieldsrem) {
					for (var j = 0; j < opt.fieldsrem.length; j++)
						value[opt.fieldsrem[j]] = undefined;
				}
				value.dbms = new QueryBuilder(self);
				value.dbms.value = value;
			}
		} else if (opt.fieldsrem) {
			if (value instanceof Array) {
				for (var i = 0; i < value.length; i++) {
					for (var j = 0; j < opt.fieldsrem.length; j++)
						value[i][opt.fieldsrem[j]] = undefined;
				}
			} else {
				for (var j = 0; j < opt.fieldsrem.length; j++)
					value[opt.fieldsrem[j]] = undefined;
			}
		}
	}

	if (err) {

		if (!self.$joinmeta && self.$) {
			self.$.invalid(err);
			self.$ = null;
		}

		try {
			opt.callback && opt.callback(err, value, count);
		} catch (e) {
			self.db.unexpected(e);
		}

		self.db.$errors.push(err);
		self.db.$lastoutput = null;
		self.db.$outputall[opt.table] = null;
		if (opt.callbackno) {
			try {
				opt.callbackno(err, opt.callbacknoparam);
			} catch (e) {
				self.db.unexpected(e);
			}
			opt.callbacknoparam = opt.callbackno = null;
		}
		self.db.$lasterror = err;
	} else {

		if (!self.$joinmeta) {
			self.db.$outputall[opt.table] = self.db.$lastoutput = value;

			if (opt.assign) {

				if (!opt.nobind) {
					if (self.db.$output == null)
						self.db.$output = {};
					self.db.$output[opt.assign] = value;
				}

				self.db.$outputall[opt.assign] = value;
			} else if (!opt.nobind)
				self.db.$output = value;

			var ok = true;
			if (opt.validate) {
				if (opt.validatereverse) {
					if (value instanceof Array) {
						if (value.length) {
							self.db.$errors.push(opt.validate);
							ok = false;
						}
					} else if (value) {
						self.db.$errors.push(opt.validate);
						ok = false;
					}
				} else {
					if (value instanceof Array) {
						if (!value.length) {
							self.db.$errors.push(opt.validate);
							ok = false;
						}
					} else if (!value) {
						self.db.$errors.push(opt.validate);
						ok = false;
					}
				}
			}
		}

		if (!self.$joinmeta && self.$) {
			err = ok ? null : opt.validate;
			if (err)
				self.$.invalid(err);
			else {
				try {
					self.$resolve(value);
				} catch (e) {
					self.db.unexpected(e);
				}
			}
			self.$reject = self.$resolve = null;
		}

		if (opt.callback) {
			try {
				opt.callback(ok ? null : opt.validate, value, count);
			} catch (e) {
				self.db.unexpected(e);
			}
		}

		if (ok) {
			if (opt.callbackok) {
				try {
					opt.callbackok(value, opt.callbackokparam);
				} catch (e) {
					self.db.unexpected(e);
				}
			}
		} else if (opt.callbackno) {
			try {
				opt.callbackno(opt.validate, opt.callbacknoparam);
			} catch (e) {
				self.db.unexpected(e);
			}
			opt.callbackno = opt.callbacknoparam = null;
		}

	}

	if (self.db.$cachekey && !iscache && !CACHEBLACKLIST[opt.type])
		exports.cache_set(self.db.$cachekey, self.db.$cacheexpire, opt.assign || 'default', { response: value, count: count });

	if (self.$orm)
		opt.callbacknoparam = opt.callbackokparam = opt.callbackok = opt.callbackno = opt.callback = undefined;

	if (!self.busy) {
		self.db.$op && clearImmediate(self.db.$op);
		self.db.$op = setImmediate(self.db.$next);
	}
};

QB.nobind = function() {
	this.options.nobind = true;
	return this;
};

QB.make = function(fn) {
	var self = this;
	fn.call(self, self);
	return self.db;
};

QB.inc = function(prop, value) {

	var self = this;

	if (self.$commandindex == null)
		throw new Error('This QueryBuilder.inc() is supported for INSERT/UPDATE/MODIFY operations.');

	var cmd = self.db.$commands[self.$commandindex];

	if (value > 0)
		prop = '+' + prop;
	else {
		prop = '-' + prop;
		value = value * -1;
	}

	if (cmd.value[prop])
		cmd.value += value;
	else
		cmd.value[prop] = value;

	return self;
};

QB.set = QB.upd = function(prop, value) {

	var self = this;

	if (value === undefined) {
		self.options.assign = prop == null ? '' : prop;
		return self;
	}

	if (self.$commandindex == null)
		throw new Error('This QueryBuilder.inc() is supported for INSERT/UPDATE/MODIFY operations.');

	self.model[prop] = value;
	return self;
};

// Is same as `set()` without `value`
QB.assign = function(prop) {
	var self = this;
	self.options.assign = prop == null ? '' : prop;
	return self;
};

QB.eq = function() {
	var model = arguments[arguments.length - 1];
	for (var i = 0; i < arguments.length - 1; i++)
		this.where(arguments[i], model[arguments[i]]);
	return this;
};

QB.inarray = function(name, value, ornull) {
	var self = this;
	if (!(value instanceof Array))
		value = [value];
	self.query((ornull ? ('array_length(' + name + ',1) IS NULL OR ') : '') + name + ' && $1', [value]);
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

QB.owner = function(name, value, member, condition) {
	var self = this;
	self.$commands.push({ type: 'owner', name: name, value: value, member: member, condition: condition });
	return self;
};

QB.permit = function(name, type, value, useridfield, userid, must) {

	// type: R read
	// type: W write
	// type: D delete

	var self = this;
	var arr = [];
	for (var i = 0; i < value.length; i++)
		arr.push(type + value[i]);

	self.$commands.push({ type: 'permit', name: name, value: arr, useridfield: useridfield, userid: userid, must: must });
	return self;
};

QB.id = function(value, field) {
	var self = this;
	if (value instanceof Array)
		self.$commands.push({ type: 'in', name: 'id', value: value, field: field });
	else
		self.$commands.push({ type: 'where', name: 'id', value: value, compare: '=' });
	return self;
};

QB.userid = function(value) {
	this.$commands.push({ type: 'where', name: 'userid', value: value, compare: '=' });
	return this;
};

QB.undeleted = function() {
	this.$commands.push({ type: 'where', name: 'isremoved=FALSE', compare: '=' });
	return this;
};

QB.in = function(name, value, field) {
	var self = this;
	self.$commands.push({ type: 'in', name: name, value: value, field: field });
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

QB.searchfull = function(value) {
	var self = this;
	self.$commands.push({ type: 'searchfull', value: value });
	return self;
};

QB.searchall = function(name, value) {
	var self = this;
	if (!(value instanceof Array))
		value = value.split(' ');
	self.$commands.push({ type: 'searchall', name: name, value: value });
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

QB.equal = function(val, model) {
	var self = this;
	var keys = val.split(',');

	if (!model)
		self.options.equal = [];

	for (var i = 0; i < keys.length; i++) {
		var k = keys[i][0] === ' ' ? keys[i].substring(1) : keys[i];
		if (model)
			self.where(k, model[k]);
		else
			self.options.equal.push(k);
	}
	return self;
};

QB.limit = function(value) {
	var self = this;
	self.options.take = value;
	return self;
};

QB.page = function(page, limit) {
	var self = this;
	if (limit)
		self.options.take = limit;
	self.options.skip = (page - 1) * self.options.take;
	return self;
};

QB.paginate = function(page, limit, maxlimit) {

	var self = this;
	var limit2 = +(limit || 0);
	var page2 = (+(page || 0)) - 1;

	if (page2 < 0 || !page2)
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

	// Because of JOINS
	if (self.$ && self.$joinmeta && self.$joinmeta.owner && !self.$joinmeta.promise) {
		self.$joinmeta.promise = true;
		self.$joinmeta.owner.$ = self.$;
		self.$joinmeta.owner.$resolve = self.$resolve;
		self.$joinmeta.owner.$reject = self.$reject;
	}

	if (self.options.callback && self.$joinmeta && self.$joinmeta.owner && !self.$joinmeta.callback) {
		self.$joinmeta.callback = true;
		self.$joinmeta.owner.options.callback = self.options.callback;
	}

	if (typeof(callback) === 'function') {
		self.options.callback = callback;
		return self;
	}

	self.$ = callback;

	return new Promise(function(resolve, reject) {
		self.$resolve = resolve;
		self.$reject = reject;
	});

};

QB.done = function($, callback, param) {
	return this.callback(function(err, response) {
		if (err)
			$.invalid(err);
		else
			callback(response, param);
	});
};

QB.debug = function() {
	this.db.$debug = debug;
	return this;
};

QB.data = function(fn, param) {
	var self = this;

	// Because of JOINS
	if (self.$joinmeta && self.$joinmeta.owner) {
		self.$joinmeta.owner.options.callbackok = fn;
		self.$joinmeta.owner.options.callbackokparam = param;
	} else {
		self.options.callbackok = fn;
		self.options.callbackokparam = param;
	}

	return self;
};

QB.fail = function(fn, param) {
	var self = this;

	// Because of JOINS
	if (self.$joinmeta && self.$joinmeta.owner) {
		self.$joinmeta.owner.options.callbackno = fn;
		self.$joinmeta.owner.options.callbacknoparam = param;
	} else {
		self.options.callbackno = fn;
		self.options.callbacknoparam = param;
	}

	return self;
};

QB.on = function(a, b) {
	var self = this;
	self.$joinmeta.a = a;
	self.$joinmeta.b = b;
	return self;
};

QB.join = function(field, table) {

	if (table == null)
		table = field;

	var self = this;
	var builder = new QueryBuilder(self.db, 'find');

	builder.table(table);
	builder.$joinmeta = { unique: new Set(), field: field, a: '', b: '', owner: self.$joinmeta ? self.$joinmeta.owner : self };

	if (self.$joins)
		self.$joins.push(builder);
	else
		self.$joins = [builder];

	self.db.$commands.push({ type: 'find', builder: builder });
	return builder;
};

QB.err = QB.error = QB.must = QB.validate = function(err, reverse) {
	var self = this;
	self.options.validate = err || 'unhandled exception';
	self.options.validatereverse = reverse;
	return self;
};

QB.insert = function(callback, params) {
	var self = this;
	self.options.insert = callback;
	self.options.insertparams = params;
	return self;
};

QB.code = QB.query = function(q, value) {
	var self = this;
	if (!self.options.params && !!value)
		self.options.params = true;
	self.$commands.push({ type: 'query', query: q, value: value });
	return self;
};

QB.or = function(fn) {
	var self = this;
	var beg = self.$commands.push({ type: 'or' });
	fn.call(self, self);
	var end = self.$commands.push({ type: 'end' });

	if ((end - beg) === 1) {
		self.$commands.pop();
		self.$commands.pop();
	}

	return self;
};

QB.subquery = function(name, query) {

	if (query == null) {
		query = name;
		name = null;
	}

	var self = this;
	if (!self.options.subquery)
		self.options.subquery = [];

	self.options.subquery.push({ name: name, query: query });
	return self;
};

QB.fields = function(fields) {

	var self = this;
	var arr = arguments;
	var is = false;

	if (arr.length === 1) {

		if (FIELDS[fields]) {
			self.options.fields = FIELDS[fields];
			return self;
		}

		if (fields.indexOf(',') !== -1) {
			arr = fields.split(',');
			is = true;
		}
	}

	if (!self.options.fields)
		self.options.fields = [];

	for (var i = 0; i < arr.length; i++) {
		var field = arr[i][0] === ' ' ? arr[i].trim() : arr[i];
		self.options.fields.push(field);
	}

	if (is)
		FIELDS[fields] = self.options.fields;

	return self;
};

QB.language = function(language, prefix, skip) {
	var self = this;
	if (skip && language && language === skip)
		language = null;
	self.options.language = (language ? ((prefix == null ? global.DBMS.languageprefix : (prefix || '')) + language) : '');
	self.options.islanguage = true;
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

QB.date = function(name, compare, value) {

	if (value === undefined) {
		value = compare;
		compare = '=';
	} else {
		compare = COMPARE[compare];
		if (compare == null)
			throw new Error('DBMS: comparer "' + compare + '" is not supported for QueryBuilder.');
	}

	var self = this;
	self.$commands.push({ type: 'date', name: name, value: value, compare: compare });
	return self;
};

// ORM
QB.wait = function() {
	var self = this;
	self.db.$skip = true;
	self.db.$op && clearImmediate(self.db.$op);
	return self;
};

// ORM
QB.remove = function(callback) {
	var self = this;
	var isnew = false;

	if (!self.db || self.db.closed) {
		isnew = true;
		self.db = new DBMS();
	}

	self.$orm = 0;

	if (self.$ormprimary) {
		self.where(self.$ormprimary, self.value[self.$ormprimary] || null);
		if (self.$ormprimaryremove)
			self.value[self.$ormprimary] = undefined;
	}

	if (self.options.callbackok)
		self.options.callbackok = null;

	if (self.options.callbackno)
		self.options.callbackno = null;

	self.options.callback = callback ? callback : null;
	self.db.$commands.push({ type: 'remove', builder: self });

	// "next" command is performend when the DBMS instance is new

	if (self.db.$skip || isnew) {
		self.db.$skip = false;
		self.db.$op && clearImmediate(self.db.$op);
		self.db.$op = setImmediate(self.db.$next);
	}

	return self;
};

// ORM
QB.continue = function() {
	var self = this;
	self.db.$skip = false;
	self.db.$op && clearImmediate(self.db.$op);
	self.db.$op = setImmediate(self.db.$next);
	return self;
};

// ORM
QB.copy = function(val, existing) {
	var self = this;
	var keys = Object.keys(val);
	for (var i = 0; i < keys.length; i++) {
		var key = keys[i];
		if (key !== 'dbms' && key !== self.$ormprimary && (!existing || self.value[key] !== undefined))
			self.value[key] = val[key];
	}
	return self;
};

// ORM
QB.modified = function(val) {
	var self = this;
	var keys = Object.keys(self.value);
	var data;

	for (var i = 0; i < keys.length; i++) {

		var key = keys[i];
		var a = val[key];

		if (a === undefined)
			continue;

		var b = self.value[key];
		if (a === b)
			continue;

		if ((a instanceof Date) && (b instanceof Date)) {
			if (a.getTime() === b.getTime())
				continue;
		} if (a && b && a instanceof Object && b instanceof Object) {
			// array or object
			if (JSON.stringify(a) === JSON.stringify(b))
				continue;
		}

		if (!data)
			data = {};

		data[key] = a;
	}

	if (data) {
		data[self.$ormprimary] = self.value[self.$ormprimary];
		self.value = data;
	} else
		self.value = null;

	return !!data;
};

QB.replace = function(val) {
	var self = this;
	var id = self.$ormprimary ? self.value[self.$ormprimary] : null;
	self.value = val;

	if (self.$ormprimary)
		self.value[self.$ormprimary] = id;

	return self;
};

// ORM
QB.save = function(callback) {
	var self = this;

	if (!self.value) {
		callback(null, 0);
		return;
	}

	var isnew = false;

	if (!self.db || self.db.closed) {
		isnew = true;
		self.db = new DBMS();
	}

	self.$orm = 0;
	self.db.$commandindex = self.db.$commands.push({ type: 'modify', builder: self }) - 1;
	self.options.fields = null;

	if (self.options.callbackok)
		self.options.callbackok = null;

	if (self.options.callbackno)
		self.options.callbackno = null;

	if (self.$ormprimary) {
		self.where(self.$ormprimary, self.value[self.$ormprimary] || null);
		if (self.$ormprimaryremove)
			self.value[self.$ormprimary] = undefined;
	}

	self.options.callback = callback ? callback : null;

	// "next" command is performed when the DBMS instance is new
	if (self.db.$skip || isnew) {
		self.db.$skip = false;
		self.db.$op && clearImmediate(self.db.$op);
		self.db.$op = setImmediate(self.db.$next);
	}

	return self;
};

exports.QueryBuilder = QueryBuilder;
exports.DBMS = DBMS;
exports.make = function(fn) {
	var self = new DBMS();
	fn.call(self, self);
	return self;
};

exports.init = function(name, connection, onerror) {

	if (connection == null) {
		connection = name;
		name = 'default';
	}

	if ((onerror === true || connection === true) && global.F) {
		onerror = function(err, sql, builder) {
			F.error(new Error(err.toString() + ': ' + sql), 'DBMS');
		};
	}

	if (connection === true || typeof(connection) === 'function') {
		if (!onerror)
			onerror = connection;
		connection = name;
		name = 'default';
		var onerror2 = onerror;
		onerror = function(err, sql, builder) {
			onerror2(new Error(err.toString() + ': ' + sql), builder);
		};
	}

	// Total.js
	if (connection === 'nosql' || connection === 'table') {
		CONN[name] = { id: name, db: 'total', type: connection };
		return exports;
	}

	if (connection === 'textdb') {
		CONN[name] = { db: 'textdb' };
		return exports;
	}

	var opt = Url.parse(connection);
	var q = Qs.parse(opt.query);
	var tmp = {};
	var arr;

	var pooling = q.pooling ? q.pooling !== '0' && q.pooling !== 'false' && q.pooling !== 'off' : true;
	var native = q.native === '1' || q.native === 'true' || q.native === 'on';

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
			tmp.native = native;
			tmp.pooling = pooling;
			CONN[name] = { id: name, db: 'pg', options: tmp, onerror: onerror, type: 'pg' };

			// Due to PG_ESCAPE
			require('./pg');

			break;
		case 'mongodb:':
		case 'mongo:':
			CONN[name] = { id: name, db: 'mongo', options: connection, database: q.database, onerror: onerror, type: 'mongodb' };
			break;
		case 'textdb:':
			CONN[name] = { id: name, db: 'textdb', options: connection, table: opt.host, database: opt.host, onerror: onerror, type: 'textdb' };
			break;
		case 'textdbhttp:':
		case 'textdbhttps:':
			var index = connection.indexOf('?token=');
			var token = connection.substring(index + 7);
			CONN[name] = { id: name, db: 'textdb', pooling: pooling, url: connection.replace('textdbhttp:', 'http:').replace('textdbhttps:', 'https:'), token: token, onerror: onerror, type: 'textdb' };
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

global.DBMS.languageprefix = '_';

global.DBMS.audit = function(fn) {
	auditwriter = fn;
};

global.DBMS.logger = function(fn) {
	if (fn === undefined)
		logger = console.log;
	else
		logger = fn;
};

global.DBMS.template = function(name, fn) {
	TEMPLATES[name] = fn;
};

global.DBMS.measure = function(callback, file) {

	if (typeof(callback) === 'boolean') {
		var tmp = file;
		file = callback;
		callback = tmp;
	}

	var stats = { insert: 0, inserttotal: 0, insertidle: 0, select: 0, selecttotal: 0, selectidle: 0, update: 0, updatetotal: 0, updateidle: 0, query: 0, querytotal: 0, queryidle: 0, delete: 0, deletetotal: 0, deleteidle: 0, count: 0, total: 0, ticks: 0 };
	var usage = {};

	ON('dbms', function(type, table, db) {

		var now = Date.now();

		stats.idle = stats.ticks ? (now - stats.ticks) : null;

		switch (type) {
			case 'insert':
				stats.insert++;
				stats.inserttotal++;
				stats.count++;
				stats.total++;
				stats.insertidle = stats.insertticks ? (now - stats.insertticks) : null;
				stats.insertticks = now;
				break;
			case 'select':
				stats.select++;
				stats.selecttotal++;
				stats.count++;
				stats.total++;
				stats.selectidle = stats.selectticks ? (now - stats.selectticks) : null;
				stats.selectticks = now;
				break;
			case 'query':
				stats.query++;
				stats.querytotal++;
				stats.count++;
				stats.total++;
				stats.queryidle = stats.queryticks ? (now - stats.queryticks) : null;
				stats.queryticks = now;
				table = table.substring(0, 30);
				break;
			case 'udpate':
				stats.update++;
				stats.updatetotal++;
				stats.count++;
				stats.total++;
				stats.updateidle = stats.updateticks ? (now - stats.updateticks) : null;
				stats.updateticks = now;
				break;
			case 'delete':
				stats['delete']++;
				stats.deletetotal++;
				stats.count++;
				stats.total++;
				stats.deleteidle = stats.deleteticks ? (now - stats.deleteticks) : null;
				stats.deleteticks = now;
				break;
		}

		stats.ticks = now;

		var key = (db === 'default' ? '' : (db + '/')) + table;
		if (!usage[key])
			usage[key] = {};

		if (usage[key][type])
			usage[key][type]++;
		else
			usage[key][type] = 1;
	});

	var createcol = function(name, size, align) {
		name = name + '';

		if (align === 2) {
			name = name.padLeft(size - 3, ' ') + ' ';
		} else {
			name = name.padRight(size - 2, ' ');
		}

		return name + '| ';
	};

	ON('service', function() {

		var keys = Object.keys(usage);
		var output = {};

		output.reqmin = 0;
		output.count = 0;
		output.insert = { reqmin: 0, count: 0, top: [] };
		output.update = { reqmin: 0, count: 0, top: [] };
		output['delete'] = { reqmin: 0, count: 0, top: [] };
		output.select = { reqmin: 0, count: 0, top: [] };
		output.query = { reqmin: 0, count: 0, top: [] };

		var count = 0;

		output.reqmin = stats.count;
		output.count = count;

		for (var i = 0; i < keys.length; i++) {
			var key = keys[i];
			var obj = usage[key];
			var keys2 = Object.keys(obj);
			for (var j = 0; j < keys2.length; j++) {
				var key2 = keys2[j];
				if (output[key2]) {
					output[key2].count += obj[key2];
					output[key2].top.push({ table: key, count: obj[key2] });
					count += obj[key2];
				}
			}
		}

		output.insert.usage = stats.insert && stats.count ? ((stats.insert / stats.count) * 100).floor(1) : 0;
		output.select.usage = stats.select && stats.count ? ((stats.select / stats.count) * 100).floor(1) : 0;
		output['delete'].usage = stats['delete'] && stats.count ? ((stats['delete'] / stats.count) * 100).floor(1) : 0;
		output.update.usage = stats.update && stats.count ? ((stats.update / stats.count) * 100).floor(1) : 0;
		output.query.usage = stats.query && stats.count ? ((stats.query / stats.count) * 100).floor(1) : 0;

		output.insert.usagetotal = stats.inserttotal && stats.total ? ((stats.inserttotal / stats.total) * 100).floor(1) : 0;
		output.select.usagetotal = stats.selecttotal && stats.total ? ((stats.selecttotal / stats.total) * 100).floor(1) : 0;
		output['delete'].usagetotal = stats.deletetotal && stats.total ? ((stats.deletetotal / stats.total) * 100).floor(1) : 0;
		output.update.usagetotal = stats.updatetotal && stats.total ? ((stats.updatetotal / stats.total) * 100).floor(1) : 0;
		output.query.usagetotal = stats.querytotal && stats.total ? ((stats.querytotal / stats.total) * 100).floor(1) : 0;

		output.insert.reqmin = stats.insert;
		output.select.reqmin = stats.select;
		output['delete'].reqmin = stats['delete'];
		output.update.reqmin = stats.update;
		output.query.reqmin = stats.query;

		for (var i = 0; i < output.insert.top.length; i++)
			output.insert.top[i].usage = ((output.insert.top[i].count / output.insert.count) * 100).floor(1);

		for (var i = 0; i < output.update.top.length; i++)
			output.update.top[i].usage = ((output.update.top[i].count / output.update.count) * 100).floor(1);

		for (var i = 0; i < output.select.top.length; i++)
			output.select.top[i].usage = ((output.select.top[i].count / output.select.count) * 100).floor(1);

		for (var i = 0; i < output['delete'].top.length; i++)
			output['delete'].top[i].usage = ((output['delete'].top[i].count / output['delete'].count) * 100).floor(1);

		for (var i = 0; i < output.query.top.length; i++)
			output.query.top[i].usage = ((output.query.top[i].count / output.query.count) * 100).floor(1);

		output.query.top.quicksort('usage', 'desc');
		output.select.top.quicksort('usage', 'desc');
		output.insert.top.quicksort('usage', 'desc');
		output.update.top.quicksort('usage', 'desc');
		output['delete'].top.quicksort('usage', 'desc');

		callback && callback(output);

		var total = stats.count;
		stats.insert = 0;
		stats.update = 0;
		stats.select = 0;
		stats['delete'] = 0;
		stats.query = 0;
		stats.count = 0;

		if (!file)
			return;

		var delimiter = '';
		var beg = '| ';
		var max = 61;
		var row = '-';

		for (var i = 0; i < max; i++) {
			delimiter += '=';
			row += '-';
		}

		delimiter = '|' + delimiter.substring(1, delimiter.length - 1) + '|';
		row = '|' + row.substring(1, row.length - 2) + '|';

		var builder = [];
		builder.push(delimiter);
		builder.push(beg + createcol(NOW.format('yyyy-MM-dd HH:mm:ss'), 24) + createcol('Req/min.', 12, 2) + createcol('Usage', 12, 2) + createcol('Total', 12, 2));
		builder.push(row);
		builder.push(beg + createcol('SELECT', 24) + createcol(output.select.reqmin, 12, 2) + createcol(output.select.usage + '%', 12, 2) + createcol(output.select.usagetotal + '%', 12, 2));
		builder.push(beg + createcol('INSERT', 24) + createcol(output.insert.reqmin, 12, 2) + createcol(output.insert.usage + '%', 12, 2) + createcol(output.insert.usagetotal + '%', 12, 2));
		builder.push(beg + createcol('UPDATE', 24) + createcol(output.update.reqmin, 12, 2) + createcol(output.update.usage + '%', 12, 2) + createcol(output.update.usagetotal + '%', 12, 2));
		builder.push(beg + createcol('DELETE', 24) + createcol(output['delete'].reqmin, 12, 2) + createcol(output['delete'].usage + '%', 12, 2) + createcol(output['delete'].usagetotal + '%', 12, 2));
		builder.push(beg + createcol('QUERY', 24) + createcol(output.query.reqmin, 12, 2) + createcol(output.query.usage + '%', 12, 2) + createcol(output.query.usagetotal + '%', 12, 2));
		builder.push(row);
		builder.push(beg + createcol('Req/min.', 36) + createcol('', 12, 2) + createcol(total, 12, 2));
		builder.push(beg + createcol('Idle time', 36) + createcol('', 12, 2) + createcol((stats.idle / 1000).floor(1) + 's', 12, 2));
		builder.push(row);

		delimiter = delimiter.substring(0, max);

		if (output.select.top.length) {
			builder.push('');
			builder.push(delimiter);
			builder.push(beg + createcol('SELECT', 36) + createcol(output.select.count, 12, 2) + createcol(output.select.usage + '%', 12, 2));
			builder.push(row);
			for (var i = 0; i < output.select.top.length; i++) {
				var tmp = output.select.top[i];
				builder.push(beg + createcol(tmp.table, 36) + createcol(tmp.count, 12, 2) + createcol(tmp.usage + '%', 12, 2));
			}
			builder.push(row);
		}

		if (output.insert.top.length) {
			builder.push('');
			builder.push(delimiter);
			builder.push(beg + createcol('INSERT', 36) + createcol(output.insert.count, 12, 2) + createcol(output.insert.usage + '%', 12, 2));
			builder.push(row);
			for (var i = 0; i < output.insert.top.length; i++) {
				var tmp = output.insert.top[i];
				builder.push(beg + createcol(tmp.table, 36) + createcol(tmp.count, 12, 2) + createcol(tmp.usage + '%', 12, 2));
			}
			builder.push(row);
		}

		if (output.update.top.length) {
			builder.push('');
			builder.push(delimiter);
			builder.push(beg + createcol('UPDATE', 36) + createcol(output.update.count, 12, 2) + createcol(output.update.usage + '%', 12, 2));
			builder.push(row);
			for (var i = 0; i < output.update.top.length; i++) {
				var tmp = output.update.top[i];
				builder.push(beg + createcol(tmp.table, 36) + createcol(tmp.count, 12, 2) + createcol(tmp.usage + '%', 12, 2));
			}
			builder.push(row);
		}

		if (output['delete'].top.length) {
			builder.push('');
			builder.push(delimiter);
			builder.push(beg + createcol('DELETE', 36) + createcol(output['delete'].count, 12, 2) + createcol(output['delete'].usage + '%', 12, 2));
			builder.push(row);
			for (var i = 0; i < output['delete'].top.length; i++) {
				var tmp = output['delete'].top[i];
				builder.push(beg + createcol(tmp.table, 36) + createcol(tmp.count, 12, 2) + createcol(tmp.usage + '%', 12, 2));
			}
			builder.push(row);
		}

		if (output.query.top.length) {
			builder.push('');
			builder.push(delimiter);
			builder.push(beg + createcol('QUERY', 36) + createcol(output.query.count, 12, 2) + createcol(output.query.usage + '%', 12, 2));
			builder.push(row);
			for (var i = 0; i < output.query.top.length; i++) {
				var tmp = output.query.top[i];
				builder.push(beg + createcol(tmp.table, 36) + createcol(tmp.count, 12, 2) + createcol(tmp.usage + '%', 12, 2));
			}
			builder.push(row);
		}

		builder.push('');

		builder.push(delimiter);
		builder.push(beg + createcol('COUNTER', 36) + createcol('Total', 24, 2));
		builder.push(row);
		builder.push(beg + createcol('SELECT', 36) + createcol(output.select.count, 24, 2));
		builder.push(beg + createcol('INSERT', 36) + createcol(output.insert.count, 24, 2));
		builder.push(beg + createcol('UPDATE', 36) + createcol(output.update.count, 24, 2));
		builder.push(beg + createcol('DELETE', 36) + createcol(output.delete.count, 24, 2));
		builder.push(beg + createcol('QUERY', 36) + createcol(output.query.count, 24, 2));
		builder.push(row);

		require('fs').writeFile(PATH.root('dbms.txt'), builder.join('\n'), NOOP);

	});

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

// Converting values
var convert = function(value, type) {

	if (type === undefined || type === String)
		return value;

	if (type === Number)
		return value.trim().parseFloat();

	if (type === Date) {
		value = value.trim();
		if (value.indexOf(' ') !== -1)
			return NOW.add('-' + value);
		if (value.length < 8) {
			var tmp;
			var index = value.indexOf('-');
			if (index !== -1) {
				tmp = value.split('-');
				value = NOW.getFullYear() + '-' + (tmp[0].length > 1 ? '' : '0') + tmp[0] + '-' + (tmp[1].length > 1 ? '' : '0') + tmp[1];
			} else {
				index = value.indexOf('.');
				if (index !== -1) {
					tmp = value.split('.');
					value = NOW.getFullYear() + '-' + (tmp[1].length > 1 ? '' : '0') + tmp[0] + '-' + (tmp[0].length > 1 ? '' : '0') + tmp[1];
				} else {
					index = value.indexOf(':');
					if (index !== -1) {
						// hours
					} else if (value.length <= 4) {
						value = +value;
						return value || 0;
					}
				}
			}
		}

		return value.trim().parseDate();
	}

	if (type === Boolean)
		return value.trim().parseBoolean();

	return value;
};

QB.transform = function(fn) {
	this.options.transform = fn;
	return this;
};

QB.gridfields = function(fields, allowed) {

	var self = this;

	if (typeof(fields) !== 'string') {
		if (allowed)
			self.options.fields = allowed.slice(0);
		return self;
	}

	fields = fields.replace(REG_FIELDS_CLEANER, '').split(',');

	if (!self.options.fields)
		self.options.fields = [];

	var count = 0;

	for (var i = 0; i < fields.length; i++) {
		var field = fields[i];
		var can = !allowed;
		if (!can) {
			for (var j = 0; j < allowed.length; j++) {
				if (allowed[j] === field) {
					can = true;
					break;
				}
			}
		}
		if (can) {
			self.options.fields.push(self.options.dbname === 'pg' ? ('"' + fields[i] + '"') : fields[i]);
			count++;
		}
	}

	if (!count)
		self.options.fields = allowed.slice(0);

	return self;
};

QB.autoquery = function(query, schema, defsort, maxlimit, localized) {

	var self = this;
	var skipped;
	var key = 'QBF' + schema;
	var allowed = CACHE[key];
	var tmp;

	if (!allowed) {
		var obj = {};
		var arr = [];
		var filter = [];

		if (localized)
			localized = localized.split(',');

		tmp = schema.split(',').trim();
		for (var i = 0; i < tmp.length; i++) {
			var k = tmp[i].split(':').trim();
			obj[k[0]] = 1;

			if (localized && localized.indexOf(k[0]) !== -1)
				arr.push(k[0] + '§');
			else
				arr.push(k[0]);

			k[1] && filter.push({ name: k[0], type: (k[1] || 'string').toLowerCase() });
		}

		allowed = CACHE[key] = { keys: arr, meta: obj, filter: filter };
	}

	var fields = query.fields;
	var fieldscount = 0;

	if (!self.options.fields)
		self.options.fields = [];

	if (fields) {
		fields = fields.replace(REG_FIELDS_CLEANER, '').split(',');
		for (var i = 0; i < fields.length; i++) {
			var field = fields[i];
			if (allowed && allowed.meta[field]) {
				self.options.fields.push(self.options.dbname === 'pg' ? ('"' + fields[i] + '"') : fields[i]);
				fieldscount++;
			}
		}
	}

	if (!fieldscount) {
		for (var i = 0; i < allowed.keys.length; i++)
			self.options.fields.push(allowed.keys[i]);
	}

	if (allowed && allowed.filter) {
		for (var i = 0; i < allowed.filter.length; i++) {
			tmp = allowed.filter[i];
			self.gridfilter(tmp.name, query, tmp.type);
		}
	}

	if (query.sort) {

		tmp = query.sort.split(',');
		var count = 0;

		for (var i = 0; i < tmp.length; i++) {
			var index = tmp[i].lastIndexOf('_');
			var name = index === - 1 ? tmp[i] : tmp[i].substring(0, index);

			if (skipped && skipped[name])
				continue;

			if (!allowed.meta[name])
				continue;

			self.sort(name, tmp[i][index + 1] === 'd');
			count++;
		}

		if (!count && defsort)
			self.gridsort(defsort);

	} else if (defsort)
		self.gridsort(defsort);

	maxlimit && self.paginate(query.page, query.limit, maxlimit);

	return self;
};

QB.autofill = function($, allowedfields, skipfilter, defsort, maxlimit, localized) {

	if (typeof(defsort) === 'number') {
		maxlimit = defsort;
		defsort = null;
	}

	var self = this;
	var query = $.query || $.options;
	var schema = $.schema;
	var skipped;
	var allowed;
	var key;
	var tmp;

	if (skipfilter) {
		key = 'QABS' + skipfilter;
		skipped = CACHE[key];
		if (!skipped) {
			tmp = skipfilter.split(',').trim();
			var obj = {};
			for (var i = 0; i < tmp.length; i++)
				obj[tmp[i]] = 1;
			skipped = CACHE[key] = obj;
		}
	}

	if (allowedfields) {
		key = 'QABF' + allowedfields;
		allowed = CACHE[key];
		if (!allowed) {
			var obj = {};
			var arr = [];
			var filter = [];

			if (localized)
				localized = localized.split(',');

			tmp = allowedfields.split(',').trim();
			for (var i = 0; i < tmp.length; i++) {
				var k = tmp[i].split(':').trim();
				obj[k[0]] = 1;

				if (localized && localized.indexOf(k[0]) !== -1)
					arr.push(k[0] + '§');
				else
					arr.push(k[0]);

				k[1] && filter.push({ name: k[0], type: (k[1] || 'string').toLowerCase() });
			}
			allowed = CACHE[key] = { keys: arr, meta: obj, filter: filter };
		}
	}

	var fields = query.fields;
	var fieldscount = 0;

	if (!self.options.fields)
		self.options.fields = [];

	if (fields) {
		fields = fields.replace(REG_FIELDS_CLEANER, '').split(',');
		for (var i = 0; i < fields.length; i++) {
			var field = fields[i];
			if (allowed && allowed.meta[field]) {
				self.options.fields.push(self.options.dbname === 'pg' ? ('"' + fields[i] + '"') : fields[i]);
				fieldscount++;
			} else if (schema.schema[field]) {
				if (skipped && skipped[field])
					continue;
				self.options.fields.push(field);
				fieldscount++;
			}
		}
	}

	if (!fieldscount) {
		if (allowed) {
			for (var i = 0; i < allowed.keys.length; i++)
				self.options.fields.push(allowed.keys[i]);
		}
		if (schema.fields) {
			for (var i = 0; i < schema.fields.length; i++) {
				if (skipped && skipped[schema.fields[i]])
					continue;
				self.options.fields.push(schema.fields[i]);
			}
		}
	}

	if (allowed && allowed.filter) {
		for (var i = 0; i < allowed.filter.length; i++) {
			tmp = allowed.filter[i];
			self.gridfilter(tmp.name, query, tmp.type);
		}
	}

	if (schema.fields) {
		for (var i = 0; i < schema.fields.length; i++) {
			var name = schema.fields[i];
			if ((!skipped || !skipped[name]) && query[name]) {
				var field = schema.schema[name];
				var type = 'string';
				switch (field.type) {
					case 2:
						type = 'number';
						break;
					case 4:
						type = 'boolean';
						break;
					case 5:
						type = 'date';
						break;
				}
				self.gridfilter(name, query, type);
			}
		}
	}

	if (query.sort) {

		tmp = query.sort.split(',');
		var count = 0;

		for (var i = 0; i < tmp.length; i++) {
			var index = tmp[i].lastIndexOf('_');
			var name = index === - 1 ? tmp[i] : tmp[i].substring(0, index);

			if (skipped && skipped[name])
				continue;

			if (allowed) {
				if (!allowed.meta[name] && !schema.schema[name])
					continue;
			} else if (!schema.schema[name])
				continue;

			self.sort(name, tmp[i][index + 1] === 'd');
			count++;
		}

		if (!count && defsort)
			self.gridsort(defsort);

	} else if (defsort)
		self.gridsort(defsort);

	maxlimit && self.paginate(query.page, query.limit, maxlimit);
	return self;
};

// Grid filtering
QB.gridfilter = function(name, obj, type, key) {

	var builder = this;
	var value = obj[name];

	if (!value)
		return builder;

	if (typeof(type) === 'string') {
		switch (type) {
			case 'number':
				type = Number;
				break;
			case 'string':
			case 'uid':
				type = String;
				break;
			case 'date':
				type = Date;
				break;
			case 'boolean':
				type = Boolean;
				break;
		}
	}

	var arr, val;

	if (!key)
		key = name;

	// Between
	if (type === Number || type === Date) {
		var index = value.indexOf(' - ');
		if (index !== -1) {

			arr = value.split(' - ');

			for (var i = 0, length = arr.length; i < length; i++) {
				var item = arr[i].trim();
				arr[i] = convert(item, type);
			}

			if (type === Date) {
				if (typeof(arr[0]) === 'number') {
					arr[0] = new Date(arr[0], 1, 1, 0, 0, 0);
					arr[1] = new Date(arr[1], 11, 31, 23, 59, 59);
				} else
					arr[1] = arr[1].extend('23:59:59');
			}

			return builder.between(key, arr[0], arr[1]);
		}
	}

	if (type === undefined || type === String) {
		var c = value[0];
		switch (c) {
			case '=':
				return builder.where(key, value.substring(1));
			case '<':
				return builder.search(key, value.substring(1), 'beg');
			case '>':
				return builder.search(key, value.substring(1), 'end');
		}

		// Multiple values
		index = value.indexOf(',');

		if (index !== -1) {
			var arr = value.split(',');
			for (var i = 0, length = arr.length; i < length; i++)
				arr[i] = convert(arr[i], type);
			return builder.in(key, arr);
		}

		return builder.search(key, value);
	}

	if (type === Date) {

		if (value === 'yesterday')
			val = NOW.add('-1 day');
		else if (value === 'today')
			val = NOW;
		else
			val = convert(value, type);

		if (typeof(val) === 'number') {
			if (val > 1000)
				return builder.year(key, val);
			else
				return builder.month(key, val);
		}

		if (!(val instanceof Date) || !val.getTime())
			val = NOW;

		return builder.between(key, val.extend('00:00:00'), val.extend('23:59:59'));
	}

	return builder.where(key, convert(value, type));
};

// Grid sorting
QB.gridsort = function(sort, one) {

	var builder = this;

	// Added multi-sort
	if (!one && sort.indexOf(',') !== -1) {
		sort = sort.split(',');
		for (var i = 0; i < sort.length; i++)
			builder.gridsort(sort[i].trim(), true);
		return builder;
	}

	var index = sort.lastIndexOf('_');
	if (index === -1)
		index = sort.lastIndexOf(' ');
	if (index === -1)
		index = sort.length;

	builder.sort(sort.substring(0, index), sort[index + 1] === 'd');
	return builder;
};

Array.prototype.dbmswait = function(onItem, callback, thread, tmp) {

	var self = this;
	var init = false;

	// INIT
	if (!tmp) {

		if (typeof(callback) !== 'function') {
			thread = callback;
			callback = null;
		}

		tmp = {};
		tmp.pending = 0;
		tmp.index = 0;
		tmp.thread = thread;

		// thread === Boolean then array has to be removed item by item

		init = true;
	}

	var item = thread === true ? self.shift() : self[tmp.index++];
	if (item === undefined) {
		if (!tmp.pending) {
			callback && callback();
			tmp.cancel = true;
		}
		return self;
	}

	tmp.pending++;
	onItem.call(self, item, () => setImmediate(next_wait, self, onItem, callback, thread, tmp), tmp.index);

	if (!init || tmp.thread === 1)
		return self;

	for (var i = 1; i < tmp.thread; i++)
		self.dbmswait(onItem, callback, 1, tmp);

	return self;
};

function next_wait(self, onItem, callback, thread, tmp) {
	tmp.pending--;
	self.dbmswait(onItem, callback, thread, tmp);
}

DP._findItems = function(items, field, value, first) {
	var arr = first ? null : [];
	for (var i = 0, length = items.length; i < length; i++) {
		if (value instanceof Array) {
			for (var j = 0; j < value.length; j++) {
				if (items[i][field] === value[j]) {
					if (first)
						return items[i];
					arr.push(items[i]);
					break;
				}
			}
		} else if (items[i][field] === value) {
			if (first)
				return items[i];
			arr.push(items[i]);
		}
	}
	return arr;
};

DP._joins = function(response, builder, count) {

	// Prepares unique values for joining
	if (response instanceof Array && response.length) {
		for (var i = 0; i < response.length; i++) {
			var item = response[i];
			for (var j = 0; j < builder.$joins.length; j++) {
				var join = builder.$joins[j];
				var meta = join.$joinmeta;
				var val = item[meta.b];
				if (val !== undefined) {
					if (val instanceof Array) {
						for (var k = 0; k < val.length; k++)
							meta.unique.add(val[k]);
					} else
						meta.unique.add(val);
				}
			}
		}
	} else if (response) {
		for (var j = 0; j < builder.$joins.length; j++) {
			var join = builder.$joins[j];
			var meta = join.$joinmeta;
			var val = response[meta.b];
			if (val !== undefined)
				meta.unique.add(val);
		}
	}

	builder.$joins.dbmswait(function(join, next) {

		var meta = join.$joinmeta;
		var arr = Array.from(meta.unique);

		meta.can = true;

		if (!arr.length) {
			join.disabled = true;
			return next();
		}

		var first = join.options.first;
		join.options.first = false;
		join.options.take = first ? 10000 : (join.options.take || 10000); // max. limit
		join.in(meta.a, arr);
		join.callback(function(err, data) {

			if (err) {
				builder.$callback(err, null, count);
				builder.$joins.length = null;
				next = null;
				return;
			}

			if (!data.length) {
				if (!first) {
					if (response instanceof Array) {
						for (var i = 0; i < response.length; i++) {
							var row = response[i];
							row[meta.field] = [];
						}
					} else
						response[meta.field] = [];
				}
			} else  if (response instanceof Array) {
				for (var i = 0; i < response.length; i++) {
					var row = response[i];
					row[meta.field] = join.db._findItems(data, meta.a, row[meta.b], first);
				}
			} else if (response)
				response[meta.field] = join.db._findItems(data, meta.a, response[meta.b], first);

			next();
		});

	}, function() {
		builder.$callback(null, response, count);
	}, 3);
};

var MYCACHE = {};
var ISCACHE = false;

exports.cache_get = function(key, prop, callback) {
	var tmp = MYCACHE[key];
	var data;
	if (tmp)
		data = CLONE(tmp.data[prop]);
	callback(null, data);
};

exports.cache_set = function(key, expire, prop, data) {
	var f = expire[0]; // first char
	// "c" clear
	// "r" remove or refresh
	if (f === 'c' || f === 'r') {
		if (MYCACHE[key])
			delete MYCACHE[key];
	} else {
		var tmp = MYCACHE[key];
		if (!tmp)
			tmp = MYCACHE[key] = { expire: NOW.add(expire), data: {} };
		tmp.data[prop] = data;
		ISCACHE = true;
	}
};

global.ON && global.ON('service', function(counter) {
	if (counter % 10 === 0)
		FIELDS = {};

	if (ISCACHE) {
		var keys = Object.keys(MYCACHE);
		var count = 0;
		for (var i = 0; i < keys.length; i++) {
			var key = keys[i];
			var item = MYCACHE[key];
			if (item.expire < NOW)
				delete MYCACHE[key];
			else
				count++;
		}
		ISCACHE = count > 0;
	}

});