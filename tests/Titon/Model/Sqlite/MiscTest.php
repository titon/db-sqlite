<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

use Titon\Model\Data\AbstractMiscTest;
use Titon\Model\Query;
use Titon\Test\Stub\Model\User;

/**
 * Test class for misc database functionality.
 */
class MiscTest extends AbstractMiscTest {

	/**
	 * Test table creation and deletion.
	 */
	public function testCreateDropTable() {
		$user = new User();

		$sql = sprintf("SELECT COUNT(name) FROM sqlite_master WHERE type = 'table' AND name = '%s';", $user->getTable());

		$this->assertEquals(0, $user->getDriver()->query($sql)->count());

		$user->createTable();

		$this->assertEquals(1, $user->getDriver()->query($sql)->count());

		$user->query(Query::DROP_TABLE)->save();

		$this->assertEquals(0, $user->getDriver()->query($sql)->count());
	}

	/**
	 * Test table truncation.
	 */
	public function testTruncateTable() {
		$this->markTestSkipped('SQLite does not support the TRUNCATE statement');
	}

	/**
	 * Test table describing.
	 */
	public function testDescribeTable() {
		$this->markTestSkipped('SQLite does not support the DESCRIBE statement');
	}

	/**
	 * Test that sub-queries return results.
	 */
	public function testSubQueries() {
		$this->loadFixtures(['Users', 'Profiles', 'Countries']);

		$user = new User();

		// SQLite does not support the ANY filter, so use IN instead
		$query = $user->select('id', 'country_id', 'username');
		$query->where('country_id', 'in', $query->subQuery('id')->from('countries'));

		$this->assertEquals([
			['id' => 1, 'country_id' => 1, 'username' => 'miles'],
			['id' => 2, 'country_id' => 3, 'username' => 'batman'],
			['id' => 3, 'country_id' => 2, 'username' => 'superman'],
			['id' => 4, 'country_id' => 5, 'username' => 'spiderman'],
			['id' => 5, 'country_id' => 4, 'username' => 'wolverine'],
		], $query->fetchAll(false));

		// Single record
		$query = $user->select('id', 'country_id', 'username');
		$query->where('country_id', '=', $query->subQuery('id')->from('countries')->where('iso', 'USA'));

		$this->assertEquals([
			['id' => 1, 'country_id' => 1, 'username' => 'miles']
		], $query->fetchAll(false));
	}

}