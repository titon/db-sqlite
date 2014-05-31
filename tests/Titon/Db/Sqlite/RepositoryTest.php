<?php
namespace Titon\Db\Sqlite;

use Titon\Db\Entity;
use Titon\Db\EntityCollection;
use Titon\Db\Query;
use Titon\Db\Query\Predicate;
use Titon\Test\Stub\Repository\Book;
use Titon\Test\Stub\Repository\Order;
use Titon\Test\Stub\Repository\Stat;

class RepositoryTest extends \Titon\Db\RepositoryTest {

    public function testCount() {
        $this->loadFixtures('Users');

        $this->assertEquals(5, $this->object->select()->count());

        $this->object->delete(1);

        $this->assertEquals(4, $this->object->select()->count());
    }

    public function testCreateMany() {
        $this->markTestSkipped('SQLite does not support multi-insert');
    }

    public function testCreateManyEntity() {
        $this->markTestSkipped('SQLite does not support multi-insert');
    }

    public function testCreateManyFiltersInvalidColumn() {
        $this->markTestSkipped('SQLite does not support multi-insert');
    }

    public function testCreateDropTable() {
        $sql = sprintf("SELECT COUNT(name) FROM sqlite_master WHERE type = 'table' AND name = '%s';", $this->object->getTable());

        $this->assertEquals(0, $this->object->getDriver()->executeQuery($sql)->count());

        $this->object->createTable();

        $this->assertEquals(1, $this->object->getDriver()->executeQuery($sql)->count());

        $this->object->dropTable();

        $this->assertEquals(0, $this->object->getDriver()->executeQuery($sql)->count());
    }

    public function testDeleteWithOrdering() {
        $this->markTestSkipped('SQLite does not support ORDER BY in DELETE');
    }

    public function testDeleteWithLimit() {
        $this->markTestSkipped('SQLite does not support LIMIT in DELETE');
    }

    public function testSelect() {
        $query = new SqliteQuery(SqliteQuery::SELECT, $this->object);
        $query->from($this->object->getTable(), 'User')->fields('id', 'username');

        $this->assertEquals($query, $this->object->select('id', 'username'));
    }

    public function testSelectRegexp() {
        $this->markTestSkipped('SQLite does not support the REGEXP clause');
    }

    public function testSelectGrouping() {
        $this->loadFixtures('Books');

        $book = new Book();

        // SQLite returns the last group record
        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 5, 'name' => 'A Dance with Dragons']),
            new Entity(['id' => 12, 'name' => 'Harry Potter and the Deathly Hallows']),
            new Entity(['id' => 15, 'name' => 'The Return of the King'])
        ]), $book->select('id', 'name')->groupBy('series_id')->orderBy('id', 'asc')->all());
    }

    public function testSelectOrdering() {
        $this->loadFixtures('Books');

        $book = new Book();

        $this->assertEquals(new EntityCollection([
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
        ]), $book->select('id', 'series_id', 'name')->orderBy([
            'series_id' => 'desc',
            'name' => 'asc'
        ])->all());
    }

    public function testSelectHavingAnd() {
        $this->loadFixtures('Orders');

        $order = new Order();
        $query = $order->select();
        $query
            ->fields([
                'id', 'user_id', 'quantity', 'status', 'shipped',
                Query::func('SUM', ['quantity' => 'field'])->asAlias('qty'),
                Query::func('COUNT', ['user_id' => 'field'])->asAlias('count')
            ])
            ->groupBy('user_id');

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 27, 'user_id' => 1, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-04-14 12:33:02', 'qty' => 97, 'count' => 5]),
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 77, 'count' => 5]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 90, 'count' => 7]),
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7]),
            new Entity(['id' => 25, 'user_id' => 5, 'quantity' => 9, 'status' => 'shipped', 'shipped' => '2013-04-30 12:33:02', 'qty' => 112, 'count' => 6]),
        ]), $query->all());

        $query->having('qty', '>', 100);

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7]),
            new Entity(['id' => 25, 'user_id' => 5, 'quantity' => 9, 'status' => 'shipped', 'shipped' => '2013-04-30 12:33:02', 'qty' => 112, 'count' => 6]),
        ]), $query->all());

        $query->having('count', '>', 6);

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7])
        ]), $query->all());
    }

    public function testSelectHavingOr() {
        $this->loadFixtures('Orders');

        $order = new Order();
        $query = $order->select();
        $query
            ->fields([
                'id', 'user_id', 'quantity', 'status', 'shipped',
                Query::func('SUM', ['quantity' => 'field'])->asAlias('qty'),
                Query::func('COUNT', ['user_id' => 'field'])->asAlias('count')
            ])
            ->groupBy('user_id');

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 27, 'user_id' => 1, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-04-14 12:33:02', 'qty' => 97, 'count' => 5]),
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 77, 'count' => 5]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 90, 'count' => 7]),
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7]),
            new Entity(['id' => 25, 'user_id' => 5, 'quantity' => 9, 'status' => 'shipped', 'shipped' => '2013-04-30 12:33:02', 'qty' => 112, 'count' => 6]),
        ]), $query->all());

        $query->orHaving('qty', '<=', 90);

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 77, 'count' => 5]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 90, 'count' => 7]),
        ]), $query->all());

        $query->orHaving('count', '>=', 6);

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 77, 'count' => 5]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 90, 'count' => 7]),
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 114, 'count' => 7]),
            new Entity(['id' => 25, 'user_id' => 5, 'quantity' => 9, 'status' => 'shipped', 'shipped' => '2013-04-30 12:33:02', 'qty' => 112, 'count' => 6]),
        ]), $query->all());
    }

    public function testSelectHavingNested() {
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
            ->having(function(Predicate $having) {
                $having->between('qty', 40, 50);
                $having->either(function(Predicate $having2) {
                    $having2->eq('status', 'shipped');
                    $having2->eq('status', 'delivered');
                });
            });

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 27, 'user_id' => 1, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-04-14 12:33:02', 'qty' => 49, 'count' => 3]),
            new Entity(['id' => 22, 'user_id' => 2, 'quantity' => 15, 'status' => 'shipped', 'shipped' => '2013-12-28 12:33:02', 'qty' => 41, 'count' => 2]),
            new Entity(['id' => 30, 'user_id' => 4, 'quantity' => 9, 'status' => 'delivered', 'shipped' => '2013-10-25 12:33:02', 'qty' => 40, 'count' => 3]),
        ]), $query->all());
    }

    public function testSelectFieldInvalidColumn() {
        $this->markTestSkipped('SQLite does not throw exceptions for invalid columns');
    }

    public function testSelectOuterJoin() {
        $this->markTestSkipped('SQLite does not support OUTER joins');
    }

    public function testSelectRightJoin() {
        $this->markTestSkipped('SQLite does not support RIGHT joins');
    }

    public function testSelectStraightJoin() {
        $this->markTestSkipped('SQLite does not support STRAIGHT joins');
    }

    public function testSelectUnions() {
        $this->loadFixtures(['Users', 'Books', 'Authors']);

        $query = $this->object->select('username AS name');
        $query->union($query->subQuery('name')->from('books')->where('series_id', 1));
        $query->union($query->subQuery('name')->from('authors'));

        // SQLite returns them in different order compared to MySQL
        $this->assertEquals(new EntityCollection([
            new Entity(['name' => 'A Clash of Kings']),
            new Entity(['name' => 'A Dance with Dragons']),
            new Entity(['name' => 'A Feast for Crows']),
            new Entity(['name' => 'A Game of Thrones']),
            new Entity(['name' => 'A Storm of Swords']),
            new Entity(['name' => 'George R. R. Martin']),
            new Entity(['name' => 'J. K. Rowling']),
            new Entity(['name' => 'J. R. R. Tolkien']),
            new Entity(['name' => 'batman']),
            new Entity(['name' => 'miles']),
            new Entity(['name' => 'spiderman']),
            new Entity(['name' => 'superman']),
            new Entity(['name' => 'wolverine']),
        ]), $query->all());

        $query->orderBy('name', 'desc')->limit(10);

        $this->assertEquals(new EntityCollection([
            new Entity(['name' => 'wolverine']),
            new Entity(['name' => 'superman']),
            new Entity(['name' => 'spiderman']),
            new Entity(['name' => 'miles']),
            new Entity(['name' => 'batman']),
            new Entity(['name' => 'J. R. R. Tolkien']),
            new Entity(['name' => 'J. K. Rowling']),
            new Entity(['name' => 'George R. R. Martin']),
            new Entity(['name' => 'A Storm of Swords']),
            new Entity(['name' => 'A Game of Thrones']),
        ]), $query->all());
    }

    public function testSelectSubQueries() {
        $this->loadFixtures(['Users', 'Profiles', 'Countries']);

        // SQLite does not support the ANY filter, so use IN instead
        $query = $this->object->select('id', 'country_id', 'username');
        $query->where('country_id', 'in', $query->subQuery('id')->from('countries'))->orderBy('id', 'asc');

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 1, 'country_id' => 1, 'username' => 'miles']),
            new Entity(['id' => 2, 'country_id' => 3, 'username' => 'batman']),
            new Entity(['id' => 3, 'country_id' => 2, 'username' => 'superman']),
            new Entity(['id' => 4, 'country_id' => 5, 'username' => 'spiderman']),
            new Entity(['id' => 5, 'country_id' => 4, 'username' => 'wolverine']),
        ]), $query->all());

        // Single record
        $query = $this->object->select('id', 'country_id', 'username');
        $query->where('country_id', '=', $query->subQuery('id')->from('countries')->where('iso', 'USA'));

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 1, 'country_id' => 1, 'username' => 'miles'])
        ]), $query->all());
    }

    public function testTruncate() {
        $this->markTestSkipped('SQLite does not support the TRUNCATE statement');
    }

    public function testUpdateMultipleWithLimit() {
        $this->markTestSkipped('SQLite does not support LIMIT in UPDATE');
    }

    public function testUpdateMultipleWithOrderBy() {
        $this->markTestSkipped('SQLite does not support ORDER BY in UPDATE');
    }

}