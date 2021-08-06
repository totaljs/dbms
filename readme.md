# Node Database Management System (ORM)


- [__Documentation__](https://docs.totaljs.com/dbms/)
- [Website](https://www.totaljs.com/)
- [__Documentation__](https://docs.totaljs.com/total4/)
- [Chat support](https://platform.totaljs.com/?open=messenger)
- [Join __Total.js Telegram__](https://t.me/totalplatform)
- [Support](https://www.totaljs.com/support/)
- supports `PostgreSQL`, `MySQL`, Total.js `TextDB` and part of `MongoDB`

## Initialization

- installation `$ npm install dbms`
	- install MySQL: `$ npm install mysql2`
	- install PostgreSQL: `$ npm install pg`
	- install MongoDB: `$ npm install mongodb`

```js
const dbms = require('dbms');

dbms.init([alias], connection_string);
// @alias {String} Optional, alias for connection string (default: 'default')
// @connection_string {String} A connection string to DB

// PostgreSQL
dbms.init('postgresql://user:pass@localhost:5432/dbname');
dbms.init('mypg', 'postgresql://user:pass@localhost:5432/dbname'); // with a name for more DB engines

// MySQL & Maria DB
dbms.init('mysql://user:pass@localhost:3306/dbname');
dbms.init('mysql', 'mysql://user:pass@localhost:3306/dbname'); // with a name for more DB engines

// Total.js NoSQL embedded
dbms.init('nosql');
dbms.init('mynosql', 'nosql'); // with a name for more DB engines

// Total.js Table
dbms.init('table');
dbms.init('mytable', 'nosql'); // with a name for more DB engines
```

## Usage

```js
// Is a global method
var db = DBMS();

// Finds records
// A response: Array
// returns QueryBuilder
db.find('collection_table_name');
db.find('mypg/collection_table_name');
db.find('mynosql/collection_table_name');
db.find('mytable/collection_table_name');

// Finds the one record
// A response: Object
// returns QueryBuilder
db.one('collection_table_name');
db.one('mypg/collection_table_name');
db.one('mynosql/collection_table_name');
db.one('mytable/collection_table_name');

// Inserts a new record
// A response: Number
// returns QueryBuilder
db.insert('collection_table_name', document, [unique]);
db.insert('mypg/collection_table_name', document, [unique]);
db.insert('mynosql/collection_table_name', document, [unique]);
db.insert('mytable/collection_table_name', document, [unique]);
```

## Contact

- Contact <https://www.totaljs.com/contact/>
- [Chat support](https://platform.totaljs.com/?open=messenger)
- [Join to __Total.js Telegram__](https://t.me/totalplatform)
