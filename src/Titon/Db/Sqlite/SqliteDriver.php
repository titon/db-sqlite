<?php
/**
 * @copyright   2010-2014, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Sqlite;

use Titon\Db\Driver\AbstractPdoDriver;
use Titon\Db\Driver\Type;
use Titon\Db\Driver\Type\AbstractType;

/**
 * A driver that represents the SQLite database and uses PDO.
 *
 * @package Titon\Db\Sqlite
 */
class SqliteDriver extends AbstractPdoDriver {

    /**
     * Configuration.
     *
     * @type array {
     *         @type string $path    Path to database file
     *         @type bool $memory    Toggle between in memory only database
     * }
     */
    protected $_config = [
        'path' => '',
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
     *
     * @uses Titon\Db\Type\AbstractType
     */
    public function describeTable($table) {
        return $this->cache([__METHOD__, $table], function() use ($table) {
            $columns = $this->executeQuery('PRAGMA table_info("' . $table  . '");')->find();
            $schema = [];

            if (!$columns) {
                return $schema;
            }

            foreach ($columns as $column) {
                $field = $column['name'];
                $type = strtolower($column['type']);
                $length = '';

                // Determine type and length
                if (preg_match('/([a-z]+)(?:\(([0-9,]+)\))?/is', $type, $matches)) {
                    $type = strtolower($matches[1]);

                    if (isset($matches[2])) {
                        $length = $matches[2];
                    }
                }

                // Inherit type defaults
                $data = $this->getType($type)->getDefaultOptions();

                // Overwrite with custom
                $data = [
                    'field' => $field,
                    'type' => $type,
                    'length' => $length,
                    'null' => !$column['notnull'],
                    'default' => $column['dflt_value']
                ] + $data;

                if ($column['pk']) {
                    $data['primary'] = true;
                    $data['ai'] = true;
                }

                if ($data['default'] === 'NULL') {
                    $data['default'] = null;
                }

                $schema[$field] = $data;
            }

            return $schema;
        });
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
        if ($dsn = $this->getConfig('dsn')) {
            return $dsn;
        }

        $dsn = $this->getDriver() . ':';

        if ($path = $this->getConfig('path')) {
            $dsn .= $path;

        } else if ($this->getConfig('memory')) {
            $dsn .= ':memory:';
        }

        return $dsn;
    }

    /**
     * {@inheritdoc}
     */
    public function getSupportedTypes() {
        return [
            'string' => 'Titon\Db\Driver\Type\StringType',
            'real' => 'Titon\Db\Driver\Type\FloatType'
        ] + parent::getSupportedTypes();
    }

    /**
     * {@inheritdoc}
     */
    public function isEnabled() {
        return extension_loaded('pdo_sqlite');
    }

    /**
     * {@inheritdoc}
     */
    public function listTables($database = null) {
        $database = $database ?: $this->getDatabase();

        return $this->cache([__METHOD__, $database], function() use ($database) {
            $tables = $this->executeQuery('SELECT * FROM sqlite_master WHERE type = ?;', ['table'])->find();
            $schema = [];

            foreach ($tables as $table) {
                if ($table['name'] === 'sqlite_sequence') {
                    continue;
                }

                $schema[] = $table['name'];
            }

            return $schema;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function newQuery($string) {
        return new SqliteQuery($string);
    }

}