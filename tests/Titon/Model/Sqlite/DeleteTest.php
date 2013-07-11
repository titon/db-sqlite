<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

use Titon\Model\Data\AbstractDeleteTest;

/**
 * Test class for database record deleting.
 */
class DeleteTest extends AbstractDeleteTest {

	/**
	 * Test delete with ordering.
	 */
	public function testDeleteOrdering() {
		$this->markTestSkipped('SQLite does not support ORDER BY in DELETE');
	}

	/**
	 * Test delete with a limit applied.
	 */
	public function testDeleteLimit() {
		$this->markTestSkipped('SQLite does not support LIMIT in DELETE');
	}

}