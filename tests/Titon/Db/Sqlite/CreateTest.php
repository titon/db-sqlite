<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Sqlite;

use Titon\Db\Data\AbstractCreateTest;

/**
 * Test class for database inserting.
 */
class CreateTest extends AbstractCreateTest {

    /**
     * Test inserting multiple records with a single statement.
     */
    public function testCreateMany() {
        $this->markTestSkipped('SQLite does not support compound multi-insert');
    }

}