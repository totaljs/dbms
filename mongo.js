const MongoDB = require('mongodb');
const MongoClient = MongoDB.MongoClient;
const EMPTYARRAY = [];
const INSERTPROJECTION = { projection: { '_id': 1 }};
const BUCKETNAME = { bucketName: 'db' };
const BLACKLIST = { dbms: 1 };

global.ObjectID = MongoDB.ObjectID;

MongoDB.ObjectID.parse = function(value) {
	try {
		return MongoDB.ObjectID.createFromHexString(value);
	} catch (e) {
		return null;
	}
};

function select(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);
	var options = {};

	var fields = FIELDS(builder);

	if (fields)
		options.projection = fields;

	if (opt.take)
		options.limit = opt.take;

	if (opt.skip)
		options.skip = opt.skip;

	if (filter.sort)
		options.sort = filter.sort;

	builder.db.$debug && builder.db.$debug({ collection: client.$database + '.' + opt.table, condition: filter.where, options: options });

	client.db(client.$database).collection(opt.table).find(filter.where, options).toArray(function(err, response) {

		client.close();
		err && client.$opt.onerror && client.$opt.onerror(err, opt, builder);

		var rows = response ? response : EMPTYARRAY;
		if (opt.first)
			rows = rows.length ? rows[0] : null;

		// checks joins
		if (!err && builder.$joins) {
			client.$dbms._join(rows, builder);
			setImmediate(builder.db.$next);
		} else
			builder.$callback(err, rows);
	});
}

function query(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);
	var options = {};

	var fields = FIELDS(builder);

	if (fields)
		options.projection = fields;

	if (opt.take)
		options.limit = opt.take;

	if (opt.skip)
		options.skip = opt.skip;

	if (filter.sort)
		options.sort = filter.sort;

	var col = client.db(client.$database).collection(cmd.query);
	cmd.value(col, function(err, response) {
		if (opt.first && response instanceof Array)
			response = response[0];
		client.close();
		err && client.$opt.onerror && client.$opt.onerror(err, opt, builder);
		builder.$callback(err, response);
	}, filter.where, options);
}

function list(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);
	var options = {};
	var fields = FIELDS(builder);

	if (fields)
		options.projection = fields;

	if (opt.take)
		options.limit = opt.take;

	if (opt.skip)
		options.skip = opt.skip;

	if (filter.sort)
		options.sort = filter.sort;

	builder.db.$debug && builder.db.$debug({ collection: client.$database + '.' + opt.table, condition: filter.where, options: options });

	var db = client.db(client.$database).collection(opt.table);
	db.countDocuments(filter.where, function(err, count) {
		if (err) {
			client.close();
			err && client.$opt.onerror && client.$opt.onerror(err, opt, builder);
			builder.$callback(err, null);
		} else {
			db.find(filter.where, options).toArray(function(err, response) {
				client.close();
				if (!err && builder.$joins) {
					client.$dbms._joins(response, builder, count || 0);
					setImmediate(builder.db.$next);
				} else
					builder.$callback(err, response, count);
			});
		}
	});
}

function scalar(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);

	// builder.db.$debug && builder.db.$debug(q);

	switch (cmd.scalar) {
		case 'count':
			client.db(client.$database).collection(opt.table).estimatedDocumentCount(filter.where, function(err, count) {
				err && client.$opt.onerror && client.$opt.onerror(err, opt, builder);
				client.close();
				builder.$callback(err, count);
			});
			break;
		case 'avg':
		case 'min':
		case 'sum':
		case 'max':
		case 'group':
			builder.$callback('Not implemented');
			break;
	}
}

function insert(client, cmd) {

	var builder = cmd.builder;
	var keys = Object.keys(cmd.builder.value);
	var opt = builder.options;
	var params = {};

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

		switch (key[0]) {
			case '-':
			case '+':
			case '*':
			case '/':
				key = key.substring(1);
				break;
		}

		params[key] = val == null ? null : typeof(val) === 'function' ? val(cmd.builder.value) : val;
	}

	// builder.db.$debug && builder.db.$debug();

	client.db(client.$database).collection(opt.table).insertOne(params, function(err, response) {
		client.close();
		err && client.$opt.onerror && client.$opt.onerror(err, opt, builder);
		builder.$callback(err, response && response.result && response.result.n ? 1 : 0);
	});
}

function insertexists(client, cmd) {
	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);

	// builder.db.$debug && builder.db.$debug(q);

	client.db(client.$database).collection(opt.table).findOne(filter.where, INSERTPROJECTION, function(err, response) {
		if (err || response) {
			client.close();
			err && client.$opt.onerror && client.$opt.onerror(err, opt, builder);
			builder.$callback(err, 0);
		} else
			insert(client, cmd);
	});
}

function modify(client, cmd) {

	var keys = Object.keys(cmd.builder.value);
	var params = null;
	var increment = null;

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

		if (typeof(val) === 'function')
			val = val(cmd.builder.value);

		switch (c) {
			case '-':
			case '+':
			case '*':
			case '/':
				!increment && (increment = {});
				key = key.substring(1);
				increment[key] = val ? val : 0;
				break;
			default:
				!params && (params = {});
				params[key] = val;
				break;
		}
	}

	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);
	// builder.db.$debug && builder.db.$debug(q);

	var upd = {};

	increment && (upd.$inc = increment);
	params && (upd.$set = params);

	var col = client.db(client.$database).collection(opt.table);
	var callback = function(err, response) {
		err && client.$opt.onerror && client.$opt.onerror(err, opt, builder);
		var count = response && response.result ? response.result.n : 0;
		if (!count && cmd.insert) {
			if (cmd.insert !== true)
				cmd.builder.value = cmd.insert;
			cmd.builder.options.insert && cmd.builder.options.insert(cmd.builder.value);
			insert(client, cmd);
		} else {
			client.close();
			builder.$callback(err, count);
		}
	};

	if (opt.first)
		col.updateOne(filter.where, upd, callback);
	else
		col.updateMany(filter.where, upd, callback);
}

function remove(client, cmd) {

	var builder = cmd.builder;
	var opt = builder.options;
	var filter = WHERE(builder);

	// builder.db.$debug && builder.db.$debug(q);

	var col = client.db(client.$database).collection(opt.table);
	var callback = function(err, response) {
		client.close();
		err && client.$opt.onerror && client.$opt.onerror(err, opt, builder);
		builder.$callback(err, response && response.result ? response.result.n : 0);
	};

	if (opt.first)
		col.deleteOne(filter.where, callback);
	else
		col.deleteMany(filter.where, callback);
}

exports.run = function(opt, self, cmd) {
	var client = new MongoClient(opt.options, { useNewUrlParser: true });
	client.connect(function(err) {

		client.$opt = opt;
		client.$dbms = self;
		client.$database = opt.database;

		if (err) {
			opt.onerror && client.$opt.onerror(err);
			if (cmd.builder)
				cmd.builder.$callback(err);
			else
				cmd.db.$next(err);
		} else {
			switch (cmd.type) {
				case 'transaction':
				case 'commit':
				case 'rollback':
					cmd.$next(new Error('"' + cmd.type + '" is not implemented.'));
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
					client.close();
					cmd.builder.$callback(new Error('Operation "' + cmd.type + '" not found'));
					break;
			}
		}
	});
};

exports.blob_read = function(opt, id, callback, conn) {
	var client = new MongoClient(opt.options, { useNewUrlParser: true });

	client.connect(function(err) {

		if (err) {
			callback(err);
			return;
		}

		if (conn.table && conn.table !== 'default')
			BUCKETNAME.bucketName = conn.table;
		else
			BUCKETNAME.bucketName = 'db';

		var done = () => client.close();
		var bucket = new MongoDB.GridFSBucket(client.db(opt.database), BUCKETNAME);
		var stream = bucket.openDownloadStream(typeof(id) === 'string' ? ObjectID.parse(id) : id);

		stream.on('error', done);
		stream.on('end', done);

		callback(null, stream);
	});
};

exports.blob_write = function(opt, stream, name, callback, conn) {
	var client = new MongoClient(opt.options, { useNewUrlParser: true });
	client.connect(function(err) {

		if (err) {
			callback(err);
			return;
		}

		if (conn.table && conn.table !== 'default')
			BUCKETNAME.bucketName = conn.table;
		else
			BUCKETNAME.bucketName = 'db';

		var options = global.U ? { contentType: U.getContentType(U.getExtension(name)) } : null;
		var bucket = new MongoDB.GridFSBucket(client.db(opt.database), BUCKETNAME);
		var writer = bucket.openUploadStream(name, options);

		stream.pipe(writer).on('error', function(err) {
			client.close();
			callback(err);
		}).on('finish', function() {
			client.close();
			callback(null, writer.id);
		});
	});
};

exports.blob_remove = function(opt, id, callback, conn) {
	var client = new MongoClient(opt.options, { useNewUrlParser: true });
	client.connect(function(err) {

		if (err) {
			callback && callback(err);
			return;
		}

		if (conn.table && conn.table !== 'default')
			BUCKETNAME.bucketName = conn.table;
		else
			BUCKETNAME.bucketName = 'db';

		var bucket = new MongoDB.GridFSBucket(client.db(opt.database), BUCKETNAME);
		bucket.delete(id, function(err) {
			client.close();
			callback && callback(err);
		});
	});
};

function eqgtlt(cmd) {
	switch (cmd.compare) {
		case '>':
			return { $gt: cmd.value };
		case '>=':
			return { $gte: cmd.value };
		case '<':
			return { $lt: cmd.value };
		case '<=':
			return { $lte: cmd.value };
	}
}

function WHERE(builder, scalar) { // , group

	var condition = {};
	var sort = null;
	var tmp = null;
	var filter;
	var value;

	for (var i = 0; i < builder.$commands.length; i++) {
		var cmd = builder.$commands[i];

		switch (cmd.type) {
			case 'where':
				value = cmd.compare === '=' ? cmd.value : eqgtlt(cmd);
				if (tmp) {
					filter = {};
					filter[cmd.name] = value;
					tmp.push(filter);
				} else
					condition[cmd.name] = value;
				break;
			case 'in':
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				value = cmd.value instanceof Array ? { $in: cmd.value } : cmd.value;
				if (tmp) {
					filter = {};
					filter[cmd.name] = value;
					tmp.push(filter);
				} else
					condition[cmd.name] = value;
				break;
			case 'notin':
				if (typeof(cmd.value) === 'function')
					cmd.value = cmd.value();
				value = cmd.value instanceof Array ? { $nin: cmd.value } : { $ne: cmd.value };
				if (tmp) {
					filter = {};
					filter[cmd.name] = value;
					tmp.push(filter);
				} else
					condition[cmd.name] = value;
				break;
			case 'between':
				value = { $gte: cmd.a, $lte: cmd.b };
				if (tmp) {
					filter = {};
					filter[cmd.name] = value;
					tmp.push(filter);
				} else
					condition[cmd.name] = value;
				break;

			case 'search':
				value = new RegExp((cmd.compare === '*' ? '' : cmd.compare === 'beg' ? '^' : '') + cmd.value + (cmd.compare === 'end' ? '$' : ''));
				if (tmp) {
					filter = {};
					filter[cmd.name] = value;
					tmp.push(filter);
				} else
					condition[cmd.name] = value;
				break;
			case 'fulltext':
				value = new RegExp((cmd.compare === '*' ? '' : cmd.compare === 'beg' ? '^' : '') + cmd.value + (cmd.compare === 'end' ? '$' : ''), 'i');
				if (tmp) {
					filter = {};
					filter[cmd.name] = value;
					tmp.push(filter);
				} else
					condition[cmd.name] = value;
				break;
			case 'contains':
				value = { $exists: true, $nin: [null, ''] };
				if (tmp) {
					filter = {};
					filter[cmd.name] = value;
					tmp.push(filter);
				} else
					condition[cmd.name] = value;
				break;
			case 'empty':
				value = { $exists: false, $in: [null, ''] };
				if (tmp) {
					filter = {};
					filter[cmd.name] = value;
					tmp.push(filter);
				} else
					condition[cmd.name] = value;
				break;
			case 'month':
			case 'year':
			case 'day':
			case 'hour':
			case 'minute':
				new Error('Not implemented', 'MongoDB doesn\'t support date function for quering.');
				break;
			case 'query':
				if (tmp) {
					filter = {};
					tmp.push(cmd.query);
				} else {
					var arr = Object.keys(cmd.query);
					for (var j = 0; j < arr.length; j++)
						condition[arr[j]] = cmd.query[arr[j]];
				}
				break;
			case 'or':
				tmp = [];
				continue;
			case 'end':
				condition.$or = tmp;
				tmp = null;
				break;
			case 'and':
				break;
			case 'sort':
				!sort && (sort = {});
				sort[cmd.name] = cmd.desc ? -1 : 1;
				break;
			case 'regexp':
				if (tmp) {
					filter = {};
					filter[cmd.name] = cmd.value;
					tmp.push(filter);
				} else
					condition[cmd.name] = cmd.value;
				break;
		}
	}

	return { where: condition, sort: sort };
}

function FIELDS(builder) {
	var output = null;
	var fields = builder.options.fields;
	if (fields && fields.length) {
		output = {};
		for (var i = 0; i < fields.length; i++) {
			var name = fields[i];
			var is = name[0] === '-';
			if (is)
				name = name.substring(1);
			output[name] = is ? 0 : 1;
		}

		if (builder.$joinmeta)
			output[builder.$joinmeta.a] = 1;
	}
	return output;
}