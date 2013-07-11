<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

use Titon\Model\Driver\Dialect\AbstractDialect;
use Titon\Model\Driver\Schema;
use Titon\Model\Driver\Type\AbstractType;
use Titon\Model\Exception\UnsupportedQueryStatementException;
use Titon\Model\Query;

/**
 * Inherit the default dialect rules and override for SQLite specific syntax.
 *
 * @package Titon\Model\Sqlite
 */
class SqliteDialect extends AbstractDialect {

	const ABORT = 'abort';
	const DEFERRABLE = 'deferrable';
	const FAIL = 'fail';
	const IGNORE = 'ignore';
	const INIT_DEFERRED = 'initiallyDeferred';
	const INIT_IMMEDIATE = 'initiallyImmediate';
	const MATCH = 'match';
	const NOT_DEFERRABLE = 'notDeferrable';
	const ON_CONFLICT = 'onConflict';
	const REPLACE = 'replace';
	const ROLLBACK = 'rollback';

	/**
	 * Configuration.
	 *
	 * @type array
	 */
	protected $_config = [
		'quoteCharacter' => '"'
	];

	/**
	 * List of full SQL statements.
	 *
	 * @type array
	 */
	protected $_statements = [
		Query::INSERT		=> 'INSERT {a.or} INTO {table} {fields} VALUES {values}',
		Query::SELECT		=> 'SELECT {a.distinct} {fields} FROM {table} {joins} {where} {groupBy} {having} {orderBy} {limit}',
		Query::UPDATE		=> 'UPDATE {a.or} {table} SET {fields} {where}',
		Query::DELETE		=> 'DELETE FROM {table} {where}',
		Query::DROP_TABLE	=> 'DROP TABLE IF EXISTS {table}',
		Query::CREATE_TABLE	=> "CREATE {a.temporary} TABLE IF NOT EXISTS {table} (\n{columns}{keys}\n)"
	];

	/**
	 * Available attributes for each query type.
	 *
	 * @type array
	 */
	protected $_attributes = [
		Query::INSERT => [
			'or' => ''
		],
		Query::SELECT => [
			'distinct' => false
		],
		Query::UPDATE => [
			'or' => ''
		],
		Query::CREATE_TABLE => [
			'temporary' => false
		]
	];

	/**
	 * Modify clauses and keywords.
	 */
	public function initialize() {
		parent::initialize();

		$this->_clauses = array_replace($this->_clauses, [
			self::DEFERRABLE		=> 'DEFERRABLE %s',
			self::EITHER 			=> 'OR %s',
			self::MATCH				=> 'MATCH %s',
			self::NOT_DEFERRABLE	=> 'NOT DEFERRABLE %s',
			self::UNIQUE_KEY		=> 'UNIQUE (%2$s)'
		]);

		$this->_keywords = array_replace($this->_keywords, [
			self::ABORT 			=> 'ABORT',
			self::AUTO_INCREMENT	=> 'AUTOINCREMENT',
			self::FAIL 				=> 'FAIL',
			self::IGNORE 			=> 'IGNORE',
			self::INIT_DEFERRED		=> 'INITIALLY DEFERRED',
			self::INIT_IMMEDIATE	=> 'INITIALLY IMMEDIATE',
			self::PRIMARY_KEY		=> 'PRIMARY KEY',
			self::REPLACE 			=> 'REPLACE',
			self::ROLLBACK 			=> 'ROLLBACK'
		]);
	}

	/**
	 * {@inheritdoc}
	 *
	 * @throws \Titon\Model\Exception\UnsupportedQueryStatementException
	 */
	public function buildMultiInsert(Query $query) {
		throw new UnsupportedQueryStatementException('SQLite does not support multi-inserts');
	}

	/**
	 * {@inheritdoc}
	 */
	public function formatColumns(Schema $schema) {
		$columns = [];

		foreach ($schema->getColumns() as $column => $options) {
			$type = $options['type'];
			$dataType = AbstractType::factory($type, $this->getDriver());

			$options = $options + $dataType->getDefaultOptions();

			// Sqlite doesn't like the shorthand version
			if ($type === 'int') {
				$type = 'integer';
			}

			if (!empty($options['length'])) {
				$type .= '(' . $options['length'] . ')';
			}

			$output = [$this->quote($column), strtoupper($type)];

			if (!empty($options['primary'])) {
				$output[] = $this->getKeyword(self::NOT_NULL);
				$output[] = $this->getKeyword(self::PRIMARY_KEY);
				$output[] = $this->getKeyword(self::AUTO_INCREMENT);

			} else {
				if (empty($options['null'])) {
					$output[] = $this->getKeyword(self::NOT_NULL);
				}

				if (!empty($options['collate'])) {
					$output[] = sprintf($this->getClause(self::COLLATE), $options['collate']);
				}

				if (array_key_exists('default', $options) && $options['default'] !== '') {
					$output[] = sprintf($this->getClause(self::DEFAULT_TO), $this->getDriver()->escape($options['default']));
				}
			}

			$columns[] = implode(' ', $output);
		}

		return implode(",\n", $columns);
	}

	/**
	 * {@inheritdoc}
	 */
	public function formatTablePrimary(array $data) {
		return ''; // Return nothing as this will be handled as a column constraint
	}

	/**
	 * {@inheritdoc}
	 */
	public function formatTableIndex($index, array $columns) {
		return ''; // SQLite does not support indices within a CREATE TABLE statement
	}

}