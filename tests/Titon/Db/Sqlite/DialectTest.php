<?php
namespace Titon\Db\Sqlite;

use Titon\Common\Config;
use Titon\Db\Driver\Dialect;
use Titon\Db\Driver\Dialect\Statement;
use Titon\Db\Driver\Schema;
use Titon\Db\Query;
use Titon\Test\Stub\Repository\User;
use \Exception;

class DialectTest extends \Titon\Db\Driver\DialectTest {

    protected function setUp() {
        $this->driver = new SqliteDriver(Config::get('db'));
        $this->driver->connect();

        $this->object = $this->driver->getDialect();
    }

    public function testAddStatements() {
        // SQLite doesn't support TRUNCATE, but just add it to fix tests
        $this->object->addStatement(Query::TRUNCATE, new Dialect\Statement('TRUNCATE {table}'));

        parent::testAddStatements();
    }

    public function testBuildCreateIndex() {
        $query = new Query(Query::CREATE_INDEX, new User());
        $query->fields('profile_id')->from('users')->asAlias('idx');

        $this->assertRegExp('/CREATE\s+INDEX IF NOT EXISTS (`|\")idx(`|\") ON (`|\")users(`|\") \((`|\")profile_id(`|\")\)/', $this->object->buildCreateIndex($query));

        $query->fields(['profile_id' => 5]);
        $this->assertRegExp('/CREATE\s+INDEX IF NOT EXISTS (`|\")idx(`|\") ON (`|\")users(`|\") \((`|\")profile_id(`|\")\(5\)\)/', $this->object->buildCreateIndex($query));

        $query->fields(['profile_id' => 'asc', 'other_id']);
        $this->assertRegExp('/CREATE\s+INDEX IF NOT EXISTS (`|\")idx(`|\") ON (`|\")users(`|\") \((`|\")profile_id(`|\") ASC, (`|\")other_id(`|\")\)/', $this->object->buildCreateIndex($query));

        $query->fields(['profile_id' => ['length' => 5, 'order' => 'desc']]);
        $this->assertRegExp('/CREATE\s+INDEX IF NOT EXISTS (`|\")idx(`|\") ON (`|\")users(`|\") \((`|\")profile_id(`|\")\(5\) DESC\)/', $this->object->buildCreateIndex($query));

        $query->fields('profile_id')->attribute('type', SqliteDialect::UNIQUE);
        $this->assertRegExp('/CREATE UNIQUE INDEX IF NOT EXISTS (`|\")idx(`|\") ON (`|\")users(`|\") \((`|\")profile_id(`|\")\)/', $this->object->buildCreateIndex($query));
    }

    public function testBuildCreateTable() {
        $schema = new Schema('foobar');
        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true
        ]);

        $query = new Query(Query::CREATE_TABLE, new User());
        $query->schema($schema);

        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")foobar(`|\") \(\n(`|\")column(`|\") INTEGER NOT NULL\n\);/', $this->object->buildCreateTable($query));

        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true,
            'primary' => true
        ]);

        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")foobar(`|\") \(\n(`|\")column(`|\") INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT\n\);/', $this->object->buildCreateTable($query));

        $schema->addColumn('column2', [
            'type' => 'int',
            'null' => true,
            'index' => true
        ]);

        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")foobar(`|\") \(\n(`|\")column(`|\") INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\n(`|\")column2(`|\") INTEGER\n\);/', $this->object->buildCreateTable($query));

        $schema->addOption('engine', 'InnoDB');

        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")foobar(`|\") \(\n(`|\")column(`|\") INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\n(`|\")column2(`|\") INTEGER\n\);/', $this->object->buildCreateTable($query));
    }

    public function testBuildDelete() {
        $query = new Query(Query::DELETE, new User());

        $query->from('foobar');
        $this->assertRegExp('/DELETE\s+FROM (`|\")foobar(`|\");/', $this->object->buildDelete($query));

        // no limit
        $query->limit(5);
        $this->assertRegExp('/DELETE\s+FROM (`|\")foobar(`|\");/', $this->object->buildDelete($query));

        $query->where('id', [1, 2, 3]);
        $this->assertRegExp('/DELETE\s+FROM (`|\")foobar(`|\")\s+WHERE (`|\")id(`|\") IN \(\?, \?, \?\);/', $this->object->buildDelete($query));

        // or order by
        $query->orderBy('id', 'asc');
        $this->assertRegExp('/DELETE\s+FROM (`|\")foobar(`|\")\s+WHERE (`|\")id(`|\") IN \(\?, \?, \?\);/', $this->object->buildDelete($query));
    }

    public function testBuildDeleteJoins() {
        $this->markTestSkipped('SQLite does not support joins in delete statements');
    }

    public function testBuildDropIndex() {
        $query = new Query(Query::DROP_INDEX, new User());
        $query->from('users')->asAlias('idx');

        $this->assertRegExp('/DROP INDEX IF EXISTS (`|\")idx(`|\")/', $this->object->buildDropIndex($query));
    }

    /**
     * @expectedException \Titon\Db\Exception\UnsupportedQueryStatementException
     */
    public function testBuildMultiInsert() {
        parent::testBuildMultiInsert();
    }

    public function testBuildUpdate() {
        $query = new Query(Query::UPDATE, new User());
        $query->from('foobar');

        $query->fields(['username' => 'miles']);
        $this->assertRegExp('/UPDATE\s+(`|\")foobar(`|\")\s+SET (`|\")username(`|\") = \?;/', $this->object->buildUpdate($query));

        // SQLite doesn't support limit
        $query->limit(15);
        $this->assertRegExp('/UPDATE\s+(`|\")foobar(`|\")\s+SET (`|\")username(`|\") = \?;/', $this->object->buildUpdate($query));

        // Or order by
        $query->orderBy('username', 'desc');
        $this->assertRegExp('/UPDATE\s+(`|\")foobar(`|\")\s+SET (`|\")username(`|\") = \?;/', $this->object->buildUpdate($query));

        $query->fields([
            'email' => 'email@domain.com',
            'website' => 'http://titon.io'
        ]);
        $this->assertRegExp('/UPDATE\s+(`|\")foobar(`|\")\s+SET (`|\")email(`|\") = \?, (`|\")website(`|\") = \?;/', $this->object->buildUpdate($query));

        $query->where('status', 3);
        $this->assertRegExp('/UPDATE\s+(`|\")foobar(`|\")\s+SET (`|\")email(`|\") = \?, (`|\")website(`|\") = \?\s+WHERE (`|\")status(`|\") = \?;/', $this->object->buildUpdate($query));
    }

    public function testBuildUpdateJoins() {
        $this->markTestSkipped('SQLite does not support joins in update statements');
    }

    public function testBuildTruncate() {
        $this->markTestSkipped('SQLite does not support the TRUNCATE statement');
    }

    public function testFormatColumns() {
        $schema = new Schema('foobar');
        $schema->addColumn('column', [
            'type' => 'int'
        ]);

        $this->assertRegExp('/(`|\")column(`|\") INTEGER/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'unsigned' => true,
            'zerofill' => true
        ]);

        $this->assertRegExp('/(`|\")column(`|\") INTEGER/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'primary' => true
        ]);

        $this->assertRegExp('/(`|\")column(`|\") INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'null' => false,
            'comment' => 'Some comment here'
        ]);

        $this->assertRegExp('/(`|\")column(`|\") INTEGER NOT NULL/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true,
            'length' => 11
        ]);

        $this->assertRegExp('/(`|\")column(`|\") INTEGER\(11\) NOT NULL/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true,
            'length' => 11,
            'unsigned' => true,
            'zerofill' => true,
            'null' => true,
            'default' => null,
            'comment' => 'Some comment here'
        ]);

        $expected = '(`|\")column(`|\") INTEGER\(11\) NOT NULL DEFAULT NULL';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));

        $schema->addColumn('column2', [
            'type' => 'varchar',
            'length' => 255,
            'null' => true
        ]);

        $expected .= ',\n(`|\")column2(`|\") VARCHAR\(255\)';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));

        $schema->addColumn('column3', [
            'type' => 'smallint',
            'default' => 3
        ]);

        $expected .= ',\n(`|\")column3(`|\") SMALLINT DEFAULT \'3\'';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));

        // inherits values from type
        $schema->addColumn('column4', [
            'type' => 'datetime'
        ]);

        $expected .= ',\n(`|\")column4(`|\") DATETIME DEFAULT NULL';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));

        $schema->addColumn('column5', [
            'type' => 'varchar',
            'collate' => SqliteDialect::BINARY,
            'charset' => 'utf8',
            'null' => false
        ]);

        $expected .= ',\n(`|\")column5(`|\") VARCHAR\(255\) NOT NULL COLLATE binary';

        $this->assertRegExp('/^' . $expected . '$/', $this->object->formatColumns($schema));
    }

    public function testFormatFieldsWithJoins() {
        $query = new Query(Query::SELECT, new User());
        $query->fields(['id', 'country_id', 'username']);
        $query->leftJoin(['countries', 'Country'], ['iso'],['users.country_id' => 'Country.id'] );

        $this->assertRegExp('/(`|\")?User(`|\")?\.(`|\")?id(`|\")? AS User__id, (`|\")?User(`|\")?\.(`|\")?country_id(`|\")? AS User__country_id, (`|\")?User(`|\")?\.(`|\")?username(`|\")? AS User__username, (`|\")?Country(`|\")?\.(`|\")?iso(`|\")? AS Country__iso/', $this->object->formatFields($query));
    }

    public function testFormatTableIndex() {
        $this->markTestSkipped('SQLite does not support CREATE TABLE statement indices');
    }

    public function testFormatTableKeys() {
        $schema = new Schema('foobar');
        $schema->addUnique('primary');

        $expected = ''; // no primary

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));

        $schema->addUnique('unique', [
            'constraint' => 'uniqueSymbol'
        ]);

        $expected .= '';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));

        $schema->addForeign('fk1', 'users.id');

        $expected .= ',\nFOREIGN KEY \((`|\")fk1(`|\")\) REFERENCES (`|\")users(`|\")\((`|\")id(`|\")\)';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));

        $schema->addForeign('fk2', [
            'references' => 'posts.id',
            'onUpdate' => Dialect::SET_NULL,
            'onDelete' => Dialect::NO_ACTION
        ]);

        $expected .= ',\nFOREIGN KEY \((`|\")fk2(`|\")\) REFERENCES (`|\")posts(`|\")\((`|\")id(`|\")\) ON UPDATE SET NULL ON DELETE NO ACTION';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));

        $schema->addIndex('column1');
        $schema->addIndex('column2');

        // no indices
        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));
    }

    public function testFormatTablePrimary() {
        $this->assertEquals('', $this->object->formatTablePrimary(['foo' => 'bar']));
    }

    public function testFormatTableUnique() {
        $data = ['columns' => ['foo'], 'constraint' => '', 'index' => 'idx'];

        $this->assertRegExp('/UNIQUE \((`|\")foo(`|\")\)/', $this->object->formatTableUnique($data));

        $data['constraint'] = 'symbol';
        $this->assertRegExp('/CONSTRAINT (`|\")symbol(`|\") UNIQUE \((`|\")foo(`|\")\)/', $this->object->formatTableUnique($data));

        $data['columns'][] = 'bar';
        $this->assertRegExp('/CONSTRAINT (`|\")symbol(`|\") UNIQUE \((`|\")foo(`|\"), (`|\")bar(`|\")\)/', $this->object->formatTableUnique($data));
    }

    public function testGetStatement() {
        $this->assertEquals(new Statement('INSERT {or} INTO {table} {fields} VALUES {values}'), $this->object->getStatement('insert'));
    }

    public function testGetStatements() {
        $this->assertEquals(['insert', 'select', 'update', 'delete', 'createTable', 'createIndex', 'dropTable', 'dropIndex'], array_keys($this->object->getStatements()));
    }

    public function testQuote() {
        $this->assertEquals('', $this->object->quote(''));
        $this->assertEquals('*', $this->object->quote('*'));
        $this->assertEquals('"foo"', $this->object->quote('foo'));
        $this->assertEquals('"foo"', $this->object->quote('foo"'));
        $this->assertEquals('"foo"', $this->object->quote('""foo"'));
        $this->assertEquals('"foo"', $this->object->quote('f"o"o'));

        $this->assertEquals('"foo"."bar"', $this->object->quote('foo.bar'));
        $this->assertEquals('"foo"."bar"', $this->object->quote('foo"."bar'));
        $this->assertEquals('"foo"."bar"', $this->object->quote('"foo"."bar"'));
        $this->assertEquals('"foo".*', $this->object->quote('foo.*'));
    }

    public function testQuoteList() {
        $this->assertEquals('"foo", "bar", "baz"', $this->object->quoteList(['foo', '"bar', '"baz"']));
        $this->assertEquals('"foo"."bar", "baz"', $this->object->quoteList(['foo.bar', '"baz"']));
    }

    public function testRenderStatement() {
        $this->assertEquals('SELECT  * FROM tableName;', $this->object->renderStatement(Query::SELECT, [
            'table' => 'tableName',
            'fields' => '*'
        ]));
    }

}