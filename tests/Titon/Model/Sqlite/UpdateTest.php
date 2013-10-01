<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Model\Sqlite;

use Titon\Model\Data\AbstractUpdateTest;

/**
 * Test class for database updating.
 */
class UpdateTest extends AbstractUpdateTest {

    /**
     * Test multiple record updates with a limit and offset applied.
     */
    public function testUpdateMultipleWithLimit() {
        $this->markTestSkipped('SQLite does not support LIMIT in UPDATE');
    }

    /**
     * Test multiple record updates with an order by applied.
     */
    public function testUpdateMultipleWithOrderBy() {
        $this->markTestSkipped('SQLite does not support ORDER BY in UPDATE');
    }

}