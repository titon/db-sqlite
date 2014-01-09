<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Sqlite;

use Titon\Db\Driver\Dialect\AbstractPdoDialect;
use Titon\Db\Driver\Schema;
use Titon\Db\Driver\Type\AbstractType;
use Titon\Db\Exception\UnsupportedFeatureException;
use Titon\Db\Query;

/**
 * Inherit the default dialect rules and override for SQLite specific syntax.
 *
 * @package Titon\Db\Sqlite
 */
class SqliteDialect extends AbstractPdoDialect {

    const ABORT = 'abort';
    const BINARY = 'binary';
    const DEFERRABLE = 'deferrable';
    const FAIL = 'fail';
    const IGNORE = 'ignore';
    const INIT_DEFERRED = 'initiallyDeferred';
    const INIT_IMMEDIATE = 'initiallyImmediate';
    const MATCH = 'match';
    const NOCASE = 'nocase';
    const NOT_DEFERRABLE = 'notDeferrable';
    const ON_CONFLICT = 'onConflict';
    const REPLACE = 'replace';
    const ROLLBACK = 'rollback';
    const RTRIM = 'rtrim';
    const UNIQUE = 'unique';

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
        Query::INSERT           => 'INSERT {a.or} INTO {table} {fields} VALUES {values}',
        Query::SELECT           => 'SELECT {a.distinct} {fields} FROM {table} {joins} {where} {groupBy} {having} {orderBy} {limit}',
        Query::UPDATE           => 'UPDATE {a.or} {table} SET {fields} {where}',
        Query::DELETE           => 'DELETE FROM {table} {where}',
        Query::CREATE_TABLE     => "CREATE {a.temporary} TABLE IF NOT EXISTS {table} (\n{columns}{keys}\n)",
        Query::CREATE_INDEX     => 'CREATE {a.type} INDEX IF NOT EXISTS {index} ON {table} ({fields})',
        Query::DROP_TABLE       => 'DROP TABLE IF EXISTS {table}',
        Query::DROP_INDEX       => 'DROP INDEX IF EXISTS {index}'
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
        ],
        Query::CREATE_INDEX => [
            'type' => ''
        ]
    ];

    /**
     * Modify clauses and keywords.
     */
    public function initialize() {
        parent::initialize();

        $this->_clauses = array_replace($this->_clauses, [
            self::DEFERRABLE        => 'DEFERRABLE %s',
            self::EITHER            => 'OR %s',
            self::MATCH             => 'MATCH %s',
            self::NOT_DEFERRABLE    => 'NOT DEFERRABLE %s',
            self::UNIQUE_KEY        => 'UNIQUE (%2$s)'
        ]);

        $this->_keywords = array_replace($this->_keywords, [
            self::ABORT             => 'ABORT',
            self::BINARY            => 'BINARY',
            self::AUTO_INCREMENT    => 'AUTOINCREMENT',
            self::FAIL              => 'FAIL',
            self::IGNORE            => 'IGNORE',
            self::INIT_DEFERRED     => 'INITIALLY DEFERRED',
            self::INIT_IMMEDIATE    => 'INITIALLY IMMEDIATE',
            self::NOCASE            => 'NOCASE',
            self::PRIMARY_KEY       => 'PRIMARY KEY',
            self::REPLACE           => 'REPLACE',
            self::ROLLBACK          => 'ROLLBACK',
            self::RTRIM             => 'RTRIM',
            self::UNIQUE            => 'UNIQUE'
        ]);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Titon\Db\Exception\UnsupportedFeatureException
     */
    public function buildMultiInsert(Query $query) {
        throw new UnsupportedFeatureException('SQLite does not support multi-inserts');
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
                if (empty($options['null']) || !empty($options['primary'])) {
                    $output[] = $this->getKeyword(self::NOT_NULL);
                }

                if (!empty($options['collate']) && in_array($options['collate'], [self::BINARY, self::NOCASE, self::RTRIM])) {
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

}