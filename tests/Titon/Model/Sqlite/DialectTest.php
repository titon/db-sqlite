<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

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

		$this->assertEquals("CREATE  TABLE IF NOT EXISTS `foobar` (\n`column` INT NOT NULL\n);", $this->object->buildCreateTable($query));

		$schema->addColumn('column', [
			'type' => 'int',
			'ai' => true,
			'primary' => true
		]);

		$this->assertEquals("CREATE  TABLE IF NOT EXISTS `foobar` (\n`column` INT NOT NULL,\nPRIMARY KEY (`column`)\n);", $this->object->buildCreateTable($query));

		$schema->addColumn('column2', [
			'type' => 'int',
			'null' => true,
			'index' => true
		]);

		$this->assertEquals("CREATE  TABLE IF NOT EXISTS `foobar` (\n`column` INT NOT NULL,\n`column2` INT NULL,\nPRIMARY KEY (`column`),\nCHECK (`column2`)\n);", $this->object->buildCreateTable($query));

		$schema->addOption('engine', 'InnoDB');

		$this->assertEquals("CREATE  TABLE IF NOT EXISTS `foobar` (\n`column` INT NOT NULL,\n`column2` INT NULL,\nPRIMARY KEY (`column`),\nCHECK (`column2`)\n);", $this->object->buildCreateTable($query));

		$query->attribute('temporary', true);

		$this->assertEquals("CREATE TEMPORARY TABLE IF NOT EXISTS `foobar` (\n`column` INT NOT NULL,\n`column2` INT NULL,\nPRIMARY KEY (`column`),\nCHECK (`column2`)\n);", $this->object->buildCreateTable($query));
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

		$this->assertEquals('`column` INT NOT NULL', $this->object->formatColumns($schema));

		$schema->addColumn('column', [
			'type' => 'int',
			'unsigned' => true,
			'zerofill' => true
		]);

		$this->assertEquals('`column` INT NOT NULL', $this->object->formatColumns($schema));

		$schema->addColumn('column', [
			'type' => 'int',
			'null' => true,
			'comment' => 'Some comment here'
		]);

		$this->assertEquals('`column` INT NULL', $this->object->formatColumns($schema));

		$schema->addColumn('column', [
			'type' => 'int',
			'ai' => true,
			'length' => 11
		]);

		$this->assertEquals('`column` INT(11) NOT NULL', $this->object->formatColumns($schema));

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

		$expected = '`column` INT(11) NULL DEFAULT NULL';

		$this->assertEquals($expected, $this->object->formatColumns($schema));

		$schema->addColumn('column2', [
			'type' => 'varchar',
			'length' => 255,
			'null' => true
		]);

		$expected .= ",\n`column2` VARCHAR(255) NULL";

		$this->assertEquals($expected, $this->object->formatColumns($schema));

		$schema->addColumn('column3', [
			'type' => 'smallint',
			'default' => 3
		]);

		$expected .= ",\n`column3` SMALLINT NOT NULL DEFAULT '3'";

		$this->assertEquals($expected, $this->object->formatColumns($schema));

		// inherits values from type
		$schema->addColumn('column4', [
			'type' => 'datetime'
		]);

		$expected .= ",\n`column4` DATETIME NULL DEFAULT NULL";

		$this->assertEquals($expected, $this->object->formatColumns($schema));

		$schema->addColumn('column5', [
			'type' => 'varchar',
			'collate' => 'utf8_general_ci',
			'charset' => 'utf8'
		]);

		$expected .= ",\n`column5` VARCHAR(255) NOT NULL COLLATE utf8_general_ci";

		$this->assertEquals($expected, $this->object->formatColumns($schema));
	}

	/**
	 * Test table keys are built with primary, unique, foreign and index.
	 */
	public function testFormatTableKeys() {
		$schema = new Schema('foobar');
		$schema->addUnique('primary');

		$expected = ",\nUNIQUE (`primary`)";

		$this->assertEquals($expected, $this->object->formatTableKeys($schema));

		$schema->addUnique('unique', [
			'constraint' => 'uniqueSymbol'
		]);

		$expected .= ",\nCONSTRAINT `uniqueSymbol` UNIQUE (`unique`)";

		$this->assertEquals($expected, $this->object->formatTableKeys($schema));

		$schema->addForeign('fk1', 'users.id');

		$expected .= ",\nFOREIGN KEY (`fk1`) REFERENCES `users`(`id`)";

		$this->assertEquals($expected, $this->object->formatTableKeys($schema));

		$schema->addForeign('fk2', [
			'references' => 'posts.id',
			'onUpdate' => Schema::SET_NULL,
			'onDelete' => Schema::NO_ACTION
		]);

		$expected .= ",\nFOREIGN KEY (`fk2`) REFERENCES `posts`(`id`) ON UPDATE SET NULL ON DELETE NO ACTION";

		$this->assertEquals($expected, $this->object->formatTableKeys($schema));

		$schema->addForeign('fk3', [
			'references' => 'posts.id',
			'match' => 'id',
			'deferrable' => SqliteDialect::INIT_DEFERRED
		]);

		$expected .= ",\nFOREIGN KEY (`fk3`) REFERENCES `posts`(`id`) MATCH `id` DEFERRABLE INITIALLY DEFERRED";

		$this->assertEquals($expected, $this->object->formatTableKeys($schema));

		$schema->addIndex('column1');
		$schema->addIndex('column2');

		$expected .= ",\nCHECK (`column1`),\nCHECK (`column2`)";

		$this->assertEquals($expected, $this->object->formatTableKeys($schema));
	}

}