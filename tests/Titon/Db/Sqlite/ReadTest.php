<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Sqlite;

use Titon\Db\Data\AbstractReadTest;
use Titon\Db\Entity;
use Titon\Db\Query\Func;
use Titon\Test\Stub\Table\Book;
use Titon\Test\Stub\Table\Order;
use Titon\Test\Stub\Table\Stat;

/**
 * Test class for database reading.
 */
class ReadTest extends AbstractReadTest {

    /**
     * Test functions in select statements.
     */
    public function testSelectFunctions() {
        $this->loadFixtures('Stats');

        $stat = new Stat();

        // SUM
        $query = $stat->select();
        $query->fields([
            $query->func('SUM', ['health' => Func::FIELD])->asAlias('sum')
        ]);

        $this->assertEquals(new Entity(['sum' => 2900]), $query->fetch());

        // SUBSTRING
        $query = $stat->select();
        $query->fields([
            $query->func('SUBSTR', ['name' => Func::FIELD, 1, 3])->asAlias('shortName')
        ]);

        $this->assertEquals([
            new Entity(['shortName' => 'War']),
            new Entity(['shortName' => 'Ran']),
            new Entity(['shortName' => 'Mag']),
        ], $query->fetchAll());

        // SUBSTRING as field in where
        $query = $stat->select('id', 'name');
        $query->where(
            $query->func('SUBSTR', ['name' => Func::FIELD, -3]),
            'ior'
        );

        $this->assertEquals([
            new Entity(['id' => 1, 'name' => 'Warrior'])
        ], $query->fetchAll());
    }

    /**
     * Test REGEXP and NOT REGEXP clauses.
     */
    public function testSelectRegexp() {
        $this->markTestSkipped('SQLite does not support the REGEXP clause');
    }

    /**
     * Test order by clause.
     */
    public function testOrdering() {
        $this->loadFixtures('Books');

        $book = new Book();

        $this->assertEquals([
            new Entity(['id' => 13, 'series_id' => 3, 'name' => 'The Fellowship of the Ring']),
            new Entity(['id' => 15, 'series_id' => 3, 'name' => 'The Return of the King']),
            new Entity(['id' => 14, 'series_id' => 3, 'name' => 'The Two Towers']),
            new Entity(['id' => 7, 'series_id' => 2, 'name' => 'Harry Potter and the Chamber of Secrets']),
            new Entity(['id' => 12, 'series_id' => 2, 'name' => 'Harry Potter and the Deathly Hallows']),
            new Entity(['id' => 9, 'series_id' => 2, 'name' => 'Harry Potter and the Goblet of Fire']),
            new Entity(['id' => 11, 'series_id' => 2, 'name' => 'Harry Potter and the Half-blood Prince']),
            new Entity(['id' => 10, 'series_id' => 2, 'name' => 'Harry Potter and the Order of the Phoenix']),
            new Entity(['id' => 6, 'series_id' => 2, 'name' => 'Harry Potter and the Philosopher\'s Stone']),
            new Entity(['id' => 8, 'series_id' => 2, 'name' => 'Harry Potter and the Prisoner of Azkaban']),
            new Entity(['id' => 2, 'series_id' => 1, 'name' => 'A Clash of Kings']),
            new Entity(['id' => 5, 'series_id' => 1, 'name' => 'A Dance with Dragons']),
            new Entity(['id' => 4, 'series_id' => 1, 'name' => 'A Feast for Crows']),
            new Entity(['id' => 1, 'series_id' => 1, 'name' => 'A Game of Thrones']),
            new Entity(['id' => 3, 'series_id' => 1, 'name' => 'A Storm of Swords']),
        ], $book->select('id', 'series_id', 'name')->orderBy([
            'series_id' => 'desc',
            'name' => 'asc'
        ])->fetchAll());
    }

    /**
     * Test group by clause.
     */
    public function testGrouping() {
        $this->loadFixtures('Books');

        $book = new Book();

        // SQLite returns the last group record
        $this->assertEquals([
            new Entity(['id' => 5, 'name' => 'A Dance with Dragons']),
            new Entity(['id' => 12, 'name' => 'Harry Potter and the Deathly Hallows']),
            new Entity(['id' => 15, 'name' => 'The Return of the King'])
        ], $book->select('id', 'name')->groupBy('series_id')->orderBy('id', 'asc')->fetchAll());
    }

    /**
     * Test having predicates using AND conjunction.
     */
    public function testHavingAnd() {
        $this->loadFixtures('Orders');

        $order = new Order();
        $query = $order->select();
        $query
            ->fields([
                'id', 'user_id', 'quantity', 'status', 'shipped',
                $query->func('SUM', ['quantity' => 'field'])->asAlias('qty'),
                $query->func('COUNT', ['user_id' => 'field'])->asAlias('count')
            ])
            ->groupBy('user_id');

        $this->assertEquals([
            new Entity(['id' => 27, 'user_id' => 1, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-04-14 12:33:02', 'qty' => 97, 'count' => 5]),
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 77, 'count' => 5]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 90, 'count' => 7]),
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7]),
            new Entity(['id' => 25, 'user_id' => 5, 'quantity' => 9, 'status' => 'shipped', 'shipped' => '2013-04-30 12:33:02', 'qty' => 112, 'count' => 6]),
        ], $query->fetchAll());

        $query->having('qty', '>', 100);

        $this->assertEquals([
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7]),
            new Entity(['id' => 25, 'user_id' => 5, 'quantity' => 9, 'status' => 'shipped', 'shipped' => '2013-04-30 12:33:02', 'qty' => 112, 'count' => 6]),
        ], $query->fetchAll());

        $query->having('count', '>', 6);

        $this->assertEquals([
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7])
        ], $query->fetchAll());
    }

    /**
     * Test having predicates using AND conjunction.
     */
    public function testHavingOr() {
        $this->loadFixtures('Orders');

        $order = new Order();
        $query = $order->select();
        $query
            ->fields([
                'id', 'user_id', 'quantity', 'status', 'shipped',
                $query->func('SUM', ['quantity' => 'field'])->asAlias('qty'),
                $query->func('COUNT', ['user_id' => 'field'])->asAlias('count')
            ])
            ->groupBy('user_id');

        $this->assertEquals([
            new Entity(['id' => 27, 'user_id' => 1, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-04-14 12:33:02', 'qty' => 97, 'count' => 5]),
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 77, 'count' => 5]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 90, 'count' => 7]),
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7]),
            new Entity(['id' => 25, 'user_id' => 5, 'quantity' => 9, 'status' => 'shipped', 'shipped' => '2013-04-30 12:33:02', 'qty' => 112, 'count' => 6]),
        ], $query->fetchAll());

        $query->orHaving('qty', '<=', 90);

        $this->assertEquals([
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 77, 'count' => 5]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 90, 'count' => 7]),
        ], $query->fetchAll());

        $query->orHaving('count', '>=', 6);

        $this->assertEquals([
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 77, 'count' => 5]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 90, 'count' => 7]),
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7]),
            new Entity(['id' => 25, 'user_id' => 5, 'quantity' => 9, 'status' => 'shipped', 'shipped' => '2013-04-30 12:33:02', 'qty' => 112, 'count' => 6]),
        ], $query->fetchAll());
    }

    /**
     * Test nested having predicates.
     */
    public function testHavingNested() {
        $this->loadFixtures('Orders');

        $order = new Order();
        $query = $order->select();
        $query
            ->fields([
                'id', 'user_id', 'quantity', 'status', 'shipped',
                $query->func('SUM', ['quantity' => 'field'])->asAlias('qty'),
                $query->func('COUNT', ['user_id' => 'field'])->asAlias('count')
            ])
            ->where('status', '!=', 'pending')
            ->groupBy('user_id')
            ->having(function() {
                $this->between('qty', 40, 50);
                $this->either(function() {
                    $this->eq('status', 'shipped');
                    $this->eq('status', 'delivered');
                });
            });

        $this->assertEquals([
            new Entity(['id' => 27, 'user_id' => 1, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-04-14 12:33:02', 'qty' => 49, 'count' => 3]),
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 41, 'count' => 2]),
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 40, 'count' => 3]),
        ], $query->fetchAll());
    }

    /**
     * Test that outer join fetches data.
     */
    public function testOuterJoin() {
        $this->markTestSkipped('SQLite does not support OUTER joins');
    }

    /**
     * Test that right join fetches data.
     */
    public function testRightJoin() {
        $this->markTestSkipped('SQLite does not support RIGHT joins');
    }

    /**
     * Test that straight join fetches data.
     */
    public function testStraightJoin() {
        $this->markTestSkipped('SQLite does not support STRAIGHT joins');
    }

}