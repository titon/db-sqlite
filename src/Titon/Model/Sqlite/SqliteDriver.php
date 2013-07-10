<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

use Titon\Model\Driver\AbstractPdoDriver;
use Titon\Model\Driver\Type;

/**
 * A driver that represents the SQLite database and uses PDO.
 *
 * @package Titon\Model\Sqlite
 */
class SqliteDriver extends AbstractPdoDriver {

	/**
	 * Configuration.
	 *
	 * @type array {
	 * 		@type bool $memory	Toggle between in memory only database
	 * }
	 */
	protected $_config = [
		'memory' => false
	];

	/**
	 * Set the dialect.
	 */
	public function initialize() {
		$this->setDialect(new SqliteDialect($this));
	}

	/**
	 * {@inheritdoc}
	 */
	public function getDriver() {
		return 'sqlite';
	}

	/**
	 * {@inheritdoc}
	 */
	public function getDsn() {
		if ($dsn = $this->config->dsn) {
			return $dsn;
		}

		$dsn = $this->getDriver() . ':';

		if ($this->config->memory) {
			$dsn .= ':memory:';
		}

		return $dsn;
	}

	/**
	 * {@inheritdoc}
	 */
	public function getSupportedTypes() {
		return [
			'string' => 'Titon\Model\Driver\Type\StringType',
			'real' => 'Titon\Model\Driver\Type\FloatType'
		] + parent::getSupportedTypes();
	}

	/**
	 * {@inheritdoc}
	 */
	public function isEnabled() {
		return extension_loaded('pdo_sqlite');
	}

}