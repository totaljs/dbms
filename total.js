!global.F && require('total.js');

exports.run = function(opt, self, cmd) {

	var db = opt.type === 'nosql' ? NOSQL : TABLE;

	switch (cmd.type) {
		case 'find':
		case 'read':
			WHERE(db(cmd.builder.options.table).find(), cmd.builder);
			break;
		case 'list':
			WHERE(db(cmd.builder.options.table).listing(), cmd.builder);
			break;
		case 'count':
			WHERE(db(cmd.builder.options.table).count(), cmd.builder);
			break;
		case 'scalar':
			WHERE(db(cmd.builder.options.table).scalar(), cmd.builder);
			break;
		case 'insert':
			WHERE(db(cmd.builder.options.table).insert(cmd.builder.value, cmd.unique), cmd.builder);
			break;
		case 'update':
			WHERE(db(cmd.builder.options.table).update(cmd.builder.value, cmd.insert), cmd.builder);
			break;
		case 'modify':
			WHERE(db(cmd.builder.options.table).modify(cmd.builder.value, cmd.insert), cmd.builder);
			break;
		case 'remove':
			WHERE(db(cmd.builder.options.table).remove(), cmd.builder);
			break;
		default:
			cmd.builder.$callback(new Error('Operation "' + cmd.type + '" not found'));
			break;
	}
};

function WHERE(db, builder) {

	builder.options.fields && db.fields.apply(db, builder.options.fields);

	for (var i = 0; i < builder.$commands.length; i++) {
		var cmd = builder.$commands[i];

		if (typeof(cmd.value) === 'function')
			cmd.value = cmd.value();

		switch (cmd.type) {
			case 'where':
				db.where(cmd.name, cmd.compare, cmd.value);
				break;
			case 'in':
				db.in(cmd.name, cmd.value);
				break;
			case 'notin':
				db.notin(cmd.name, cmd.value);
				break;
			case 'code':
				db.code(cmd.value);
				break;
			case 'between':
				if (typeof(cmd.a) === 'function')
					cmd.a = cmd.a();
				if (typeof(cmd.b) === 'function')
					cmd.b = cmd.b();
				db.between(cmd.name, cmd.a, cmd.b);
				break;
			case 'search':
				db.search(cmd.name, cmd.value, cmd.compare);
				break;
			case 'fulltext':
				db.fulltext(cmd.name, cmd.value, cmd.weight);
				break;
			case 'contains':
				db.contains(cmd.name);
				break;
			case 'empty':
				db.empty(cmd.name);
				break;
			case 'year':
				db.year(cmd.name, cmd.compare, cmd.value);
				break;
			case 'month':
				db.month(cmd.name, cmd.compare, cmd.value);
				break;
			case 'day':
				db.day(cmd.name, cmd.compare, cmd.value);
				break;
			case 'or':
				db.or();
				break;
			case 'end':
				db.end();
				break;
			case 'and':
				db.and();
				break;
			case 'sort':
				db.sort(cmd.name, cmd.desc);
				break;
			case 'regexp':
				db.regexp(cmd.name, cmd.value);
				break;
		}
	}

	builder.options.skip && db.skip(builder.options.skip);
	builder.options.take && db.take(builder.options.take);
	builder.options.first && db.first();
	db.callback(function(err, response, count) {
		builder.$callback(err, response, count);
	});
}