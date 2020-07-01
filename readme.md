# Node Database Management System (ORM)

[![Professional Support](https://www.totaljs.com/img/badge-support.svg)](https://www.totaljs.com/support/) [![Chat with contributors](https://www.totaljs.com/img/badge-chat.svg)](https://messenger.totaljs.com) [![NPM version][npm-version-image]][npm-url] [![NPM downloads][npm-downloads-image]][npm-url] [![MIT License][license-image]][license-url]

- [__Documentation__](https://wiki.totaljs.com/dbms/01-welcome/)
- installation `$ npm install dbms`
- supports Total.js `NoSQL embedded`
- supports `PostgreSQL` and `MongoDB`

## Initialization

```javascript
const dbms = require('dbms');

dbms.init([alias], connection_string);
// @alias {String} Optional, alias for connection string (default: 'default')
// @connection_string {String} A connection string to DB

// PostgreSQL
dbms.init('postgresql://user:pass@localhost:5432/dbname');
dbms.init('mypg', 'postgresql://user:pass@localhost:5432/dbname'); // with a name for more DB engines

// Total.js NoSQL embedded
dbms.init('nosql');
dbms.init('mynosql', 'nosql'); // with a name for more DB engines

// Total.js Table
dbms.init('table');
dbms.init('mytable', 'nosql'); // with a name for more DB engines
```

## Simple usage

```javascript
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

Do you have any questions? Contact us <https://www.totaljs.com/contact/>

[![Professional Support](https://www.totaljs.com/img/badge-support.svg)](https://www.totaljs.com/support/) [![Chat with contributors](https://www.totaljs.com/img/badge-chat.svg)](https://messenger.totaljs.com)

[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat
[license-url]: license.txt

[npm-url]: https://npmjs.org/package/sqlagent
[npm-version-image]: https://img.shields.io/npm/v/sqlagent.svg?style=flat
[npm-downloads-image]: https://img.shields.io/npm/dm/sqlagent.svg?style=flat
