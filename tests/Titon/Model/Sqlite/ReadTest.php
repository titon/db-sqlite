<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

use Titon\Model\Data\AbstractReadTest;
use Titon\Model\Entity;
use Titon\Model\Query\Func;
use Titon\Test\Stub\Model\Book;
use Titon\Test\Stub\Model\Stat;

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

		$this->assertEquals(['sum' => 2900], $query->fetch(false));

		// SUBSTRING
		$query = $stat->select();
		$query->fields([
			$query->func('SUBSTR', ['name' => Func::FIELD, 1, 3])->asAlias('shortName')
		]);

		$this->assertEquals([
			['shortName' => 'War'],
			['shortName' => 'Ran'],
			['shortName' => 'Mag'],
		], $query->fetchAll(false));

		// SUBSTRING as field in where
		$query = $stat->select('id', 'name');
		$query->where(
			$query->func('SUBSTR', ['name' => Func::FIELD, -3]),
			'ior'
		);

		$this->assertEquals([
			['id' => 1, 'name' => 'Warrior']
		], $query->fetchAll(false));
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

}