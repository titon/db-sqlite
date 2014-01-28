<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Sqlite;

use DateTime;
use Titon\Db\Data\AbstractMiscTest;
use Titon\Db\Entity;
use Titon\Db\Query;
use Titon\Test\Stub\Repository\Stat;
use Titon\Test\Stub\Repository\User;

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
     * Test that sub-queries return results.
     */
    public function testSubQueries() {
        $this->loadFixtures(['Users', 'Profiles', 'Countries']);

        $user = new User();

        // SQLite does not support the ANY filter, so use IN instead
        $query = $user->select('id', 'country_id', 'username');
        $query->where('country_id', 'in', $query->subQuery('id')->from('countries'))->orderBy('id', 'asc');

        $this->assertEquals([
            new Entity(['id' => 1, 'country_id' => 1, 'username' => 'miles']),
            new Entity(['id' => 2, 'country_id' => 3, 'username' => 'batman']),
            new Entity(['id' => 3, 'country_id' => 2, 'username' => 'superman']),
            new Entity(['id' => 4, 'country_id' => 5, 'username' => 'spiderman']),
            new Entity(['id' => 5, 'country_id' => 4, 'username' => 'wolverine']),
        ], $query->fetchAll());

        // Single record
        $query = $user->select('id', 'country_id', 'username');
        $query->where('country_id', '=', $query->subQuery('id')->from('countries')->where('iso', 'USA'));

        $this->assertEquals([
            new Entity(['id' => 1, 'country_id' => 1, 'username' => 'miles'])
        ], $query->fetchAll());
    }

    /**
     * Test type casting for insert fields.
     */
    public function testInsertFieldTypeCasting() {
        $this->loadFixtures('Stats', 'Users');

        $stat = new Stat();
        $user = new User();
        $time = time();
        $date = date('Y-m-d H:i:s', $time);
        $driver = $stat->getDriver();

        // int
        $query = $driver->query($stat->query(Query::INSERT)->fields(['health' => '100', 'energy' => 200]));
        $this->assertRegExp("/^INSERT INTO \"stats\" \(\"health\", \"energy\"\) VALUES \(100, 200\);$/i", $query->getStatement());

        // string
        $query = $driver->query($stat->query(Query::INSERT)->fields(['name' => 12345]));
        $this->assertRegExp("/^INSERT INTO \"stats\" \(\"name\"\) VALUES \('12345'\);$/i", $query->getStatement());

        // float, double, decimal (they are strings in PDO)
        $query = $driver->query($stat->query(Query::INSERT)->fields(['damage' => '123.45', 'defense' => 456.78, 'range' => 999.00]));
        $this->assertRegExp("/^INSERT INTO \"stats\" \(\"damage\", \"defense\", \"range\"\) VALUES \('123.45', '456.78', '999'\);$/i", $query->getStatement());

        // bool
        $query = $driver->query($stat->query(Query::INSERT)->fields(['isMelee' => 'true']));
        $this->assertRegExp("/^INSERT INTO \"stats\" \(\"isMelee\"\) VALUES \(1\);$/i", $query->getStatement());

        $query = $driver->query($stat->query(Query::INSERT)->fields(['isMelee' => false]));
        $this->assertRegExp("/^INSERT INTO \"stats\" \(\"isMelee\"\) VALUES \(0\);$/i", $query->getStatement());

        // datetime
        $query = $driver->query($user->query(Query::INSERT)->fields(['created' => $time]));
        $this->assertRegExp("/^INSERT INTO \"users\" \(\"created\"\) VALUES \('" . $date . "'\);$/i", $query->getStatement());

        $query = $driver->query($user->query(Query::INSERT)->fields(['created' => new DateTime($date)]));
        $this->assertRegExp("/^INSERT INTO \"users\" \(\"created\"\) VALUES \('" . $date . "'\);$/i", $query->getStatement());

        $query = $driver->query($user->query(Query::INSERT)->fields(['created' => $date]));
        $this->assertRegExp("/^INSERT INTO \"users\" \(\"created\"\) VALUES \('" . $date . "'\);$/i", $query->getStatement());

        // null
        $query = $driver->query($user->query(Query::INSERT)->fields(['created' => null]));
        $this->assertRegExp("/^INSERT INTO \"users\" \(\"created\"\) VALUES \(NULL\);$/i", $query->getStatement());
    }

    /**
     * Test type casting for update fields.
     */
    public function testUpdateFieldTypeCasting() {
        $this->loadFixtures('Stats', 'Users');

        $stat = new Stat();
        $user = new User();
        $time = time();
        $date = date('Y-m-d H:i:s', $time);
        $driver = $stat->getDriver();

        // int
        $query = $driver->query($stat->query(Query::UPDATE)->fields(['health' => '100', 'energy' => 200]));
        $this->assertRegExp("/^UPDATE \"stats\" SET \"health\" = 100, \"energy\" = 200;$/i", $query->getStatement());

        // string
        $query = $driver->query($stat->query(Query::UPDATE)->fields(['name' => 12345]));
        $this->assertRegExp("/^UPDATE \"stats\" SET \"name\" = '12345';$/i", $query->getStatement());

        // float, double, decimal (they are strings in PDO)
        $query = $driver->query($stat->query(Query::UPDATE)->fields(['damage' => '123.45', 'defense' => 456.78, 'range' => 999.00]));
        $this->assertRegExp("/^UPDATE \"stats\" SET \"damage\" = '123.45', \"defense\" = '456.78', \"range\" = '999';$/i", $query->getStatement());

        // bool
        $query = $driver->query($stat->query(Query::UPDATE)->fields(['isMelee' => 'true']));
        $this->assertRegExp("/^UPDATE \"stats\" SET \"isMelee\" = 1;$/i", $query->getStatement());

        $query = $driver->query($stat->query(Query::UPDATE)->fields(['isMelee' => false]));
        $this->assertRegExp("/^UPDATE \"stats\" SET \"isMelee\" = 0;$/i", $query->getStatement());

        // datetime
        $query = $driver->query($user->query(Query::UPDATE)->fields(['created' => $time]));
        $this->assertRegExp("/^UPDATE \"users\" SET \"created\" = '" . $date . "';$/i", $query->getStatement());

        $query = $driver->query($user->query(Query::UPDATE)->fields(['created' => new DateTime($date)]));
        $this->assertRegExp("/^UPDATE \"users\" SET \"created\" = '" . $date . "';$/i", $query->getStatement());

        $query = $driver->query($user->query(Query::UPDATE)->fields(['created' => $date]));
        $this->assertRegExp("/^UPDATE \"users\" SET \"created\" = '" . $date . "';$/i", $query->getStatement());

        // null
        $query = $driver->query($user->query(Query::UPDATE)->fields(['created' => null]));
        $this->assertRegExp("/^UPDATE \"users\" SET \"created\" = NULL;$/i", $query->getStatement());
    }

    /**
     * Test type casting in where clauses.
     */
    public function testWhereTypeCasting() {
        $this->loadFixtures('Stats', 'Users');

        $stat = new Stat();
        $user = new User();
        $time = time();
        $date = date('Y-m-d H:i:s', $time);
        $driver = $stat->getDriver();

        // int
        $query = $driver->query($stat->select()->where('health', '>', '100'));
        $this->assertRegExp("/^SELECT \* FROM \"stats\" WHERE \"health\" > 100;$/i", $query->getStatement());

        $query = $driver->query($stat->select()->where('id', [1, '2', 3]));
        $this->assertRegExp("/^SELECT \* FROM \"stats\" WHERE \"id\" IN \(1, 2, 3\);$/i", $query->getStatement());

        // string
        $query = $driver->query($stat->select()->where('name', '!=', 123.45));
        $this->assertRegExp("/^SELECT \* FROM \"stats\" WHERE \"name\" != '123.45';$/i", $query->getStatement());

        // float (they are strings in PDO)
        $query = $driver->query($stat->select()->where('damage', '<', 55.25));
        $this->assertRegExp("/^SELECT \* FROM \"stats\" WHERE \"damage\" < '55.25';$/i", $query->getStatement());

        // bool
        $query = $driver->query($stat->select()->where('isMelee', true));
        $this->assertRegExp("/^SELECT \* FROM \"stats\" WHERE \"isMelee\" = 1;$/i", $query->getStatement());

        $query = $driver->query($stat->select()->where('isMelee', '0'));
        $this->assertRegExp("/^SELECT \* FROM \"stats\" WHERE \"isMelee\" = 0;$/i", $query->getStatement());

        // datetime
        $query = $driver->query($user->select()->where('created', '>', $time));
        $this->assertRegExp("/^SELECT \* FROM \"users\" WHERE \"created\" > '" . $date . "';$/i", $query->getStatement());

        $query = $driver->query($user->select()->where('created', '<=', new DateTime($date)));
        $this->assertRegExp("/^SELECT \* FROM \"users\" WHERE \"created\" <= '" . $date . "';$/i", $query->getStatement());

        $query = $driver->query($user->select()->where('created', '!=', $date));
        $this->assertRegExp("/^SELECT \* FROM \"users\" WHERE \"created\" != '" . $date . "';$/i", $query->getStatement());

        // null
        $query = $driver->query($user->select()->where('created', null));
        $this->assertRegExp("/^SELECT \* FROM \"users\" WHERE \"created\" IS NULL;$/i", $query->getStatement());

        $query = $driver->query($user->select()->where('created', '!=', null));
        $this->assertRegExp("/^SELECT \* FROM \"users\" WHERE \"created\" IS NOT NULL;$/i", $query->getStatement());
    }

}