<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

use Exception;
use Titon\Model\Driver\Dialect;
use Titon\Model\Driver\Schema;
use Titon\Model\Query;
use Titon\Test\Stub\DriverStub;
use Titon\Test\Stub\Model\User;

/**
 * Test class for database record deleting.
 */
class DialectTest extends \Titon\Model\Driver\DialectTest {

	/**
	 * This method is called before a test is executed.
	 */
	protected function setUp() {
		parent::setUp();

		$this->driver = new DriverStub('default', []);
		$this->driver->connect();

		$this->object = new SqliteDialect($this->driver);
	}

	/**
	 * Test create table statement creation.
	 */
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

	/**
	 * Test delete statement creation.
	 */
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

	/**
	 * Test delete statements that contain joins.
	 */
	public function testBuildDeleteJoins() {
		$this->markTestSkipped('SQLite does not support joins in delete statements');
	}

	/**
	 * Test describe statement creation.
	 */
	public function testBuildDescribe() {
		$this->markTestSkipped('SQLite does not support the DESCRIBE statement');
	}

	/**
	 * Test multi insert statement creation.
	 */
	public function testBuildMultiInsert() {
		$this->markTestSkipped('SQLite does not support compound multi-insert');
	}

	/**
	 * Test update statement creation.
	 */
	public function testBuildUpdate() {
		$query = new Query(Query::UPDATE, new User());

		// No fields
		try {
			$this->object->buildUpdate($query);
			$this->assertTrue(false);
		} catch (Exception $e) {
			$this->assertTrue(true);
		}

		$query->fields(['username' => 'miles']);

		// No table
		try {
			$this->object->buildUpdate($query);
			$this->assertTrue(false);
		} catch (Exception $e) {
			$this->assertTrue(true);
		}

		$query->from('foobar');
		$this->assertRegExp('/UPDATE\s+(`|\")foobar(`|\")\s+SET (`|\")username(`|\") = \?;/', $this->object->buildUpdate($query));

		// no limit
		$query->limit(15);
		$this->assertRegExp('/UPDATE\s+(`|\")foobar(`|\")\s+SET (`|\")username(`|\") = \?;/', $this->object->buildUpdate($query));

		// or order by
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

	/**
	 * Test update statements that contain joins.
	 */
	public function testBuildUpdateJoins() {
		$this->markTestSkipped('SQLite does not support joins in update statements');
	}

	/**
	 * Test truncate table statement creation.
	 */
	public function testBuildTruncate() {
		$this->markTestSkipped('SQLite does not support the TRUNCATE statement');
	}

	/**
	 * Test table column formatting builds according to the options defined.
	 */
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
			'collate' => 'utf8_general_ci',
			'charset' => 'utf8',
			'null' => false
		]);

		$expected .= ',\n(`|\")column5(`|\") VARCHAR\(255\) NOT NULL COLLATE utf8_general_ci';

		$this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));
	}

	/**
	 * Test index keys.
	 */
	public function testFormatTableIndex() {
		$this->markTestSkipped('SQLite does not support CREATE TABLE statement indices');
	}

	/**
	 * Test table keys are built with primary, unique, foreign and index.
	 */
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

	/**
	 * Test primary key.
	 */
	public function testFormatTablePrimary() {
		$this->markTestSkipped('Purposefully not implementing primary key constraints');
	}

	/**
	 * Test unique keys.
	 */
	public function testFormatTableUnique() {
		$data = ['columns' => ['foo'], 'constraint' => '', 'index' => 'idx'];

		$this->assertRegExp('/UNIQUE \((`|\")foo(`|\")\)/', $this->object->formatTableUnique($data));

		$data['constraint'] = 'symbol';
		$this->assertRegExp('/CONSTRAINT (`|\")symbol(`|\") UNIQUE \((`|\")foo(`|\")\)/', $this->object->formatTableUnique($data));

		$data['columns'][] = 'bar';
		$this->assertRegExp('/CONSTRAINT (`|\")symbol(`|\") UNIQUE \((`|\")foo(`|\"), (`|\")bar(`|\")\)/', $this->object->formatTableUnique($data));
	}

	/**
	 * Test identifier quoting.
	 */
	public function testQuote() {
		$this->assertEquals('"foo"', $this->object->quote('foo'));
		$this->assertEquals('"foo"', $this->object->quote('foo"'));
		$this->assertEquals('"foo"', $this->object->quote('""foo"'));

		$this->assertEquals('"foo"."bar"', $this->object->quote('foo.bar'));
		$this->assertEquals('"foo"."bar"', $this->object->quote('foo"."bar'));
		$this->assertEquals('"foo"."bar"', $this->object->quote('"foo"."bar"'));
		$this->assertEquals('"foo".*', $this->object->quote('foo.*'));
	}

	/**
	 * Test multiple identifier quoting.
	 */
	public function testQuoteList() {
		$this->assertEquals('"foo", "bar", "baz"', $this->object->quoteList(['foo', '"bar', '"baz"']));
		$this->assertEquals('"foo"."bar", "baz"', $this->object->quoteList(['foo.bar', '"baz"']));
	}

}