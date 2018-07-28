var DB = require('./index');

// DB.init('postgres://localhost:5432/cynapse_connect');
DB.init('table');
//DB.init('table', 'table');

var db = DBMS();
db.debug();
db.find('products').assign('items').fields('id', 'name', 'publisherid');
db.one('publishers').in('id', db.data('products.publisherid')).assign('detail').fields('name');
db.callback(function(err, response) {
	console.log(err, response);
	process.exit(0);
});


/*
DBMS.make(function(db) {
	// db.find('tbl_user').callback(console.log);
	// db.update('cl_status', { id: 5, name: 'PIČKA2' }, true).where('name', 'PIČKA').callback(console.log);
	// db.scalar('tbl_user', 'avg', 'countlogin').callback(console.log);
	db.callback(() => process.exit(0));
});
*/