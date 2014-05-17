<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Sqlite;

use Titon\Db\Driver\Dialect\AbstractPdoDialect;
use Titon\Db\Driver\Dialect\Statement;
use Titon\Db\Driver\Schema;
use Titon\Db\Exception\UnsupportedQueryStatementException;
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
        'quoteCharacter' => '"',
        'virtualJoins' => true
    ];

    /**
     * Modify clauses and keywords.
     */
    public function initialize() {
        parent::initialize();

        $this->addClauses([
            self::DEFERRABLE        => 'DEFERRABLE %s',
            self::EITHER            => 'OR %s',
            self::MATCH             => 'MATCH %s',
            self::NOT_DEFERRABLE    => 'NOT DEFERRABLE %s',
            self::UNIQUE_KEY        => 'UNIQUE (%2$s)'
        ]);

        $this->addKeywords([
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

        $this->addStatements([
            Query::INSERT        => new Statement('INSERT {or} INTO {table} {fields} VALUES {values}'),
            Query::SELECT        => new Statement('SELECT {distinct} {fields} FROM {table} {joins} {where} {groupBy} {having} {compounds} {orderBy} {limit}'),
            Query::UPDATE        => new Statement('UPDATE {or} {table} SET {fields} {where}'),
            Query::DELETE        => new Statement('DELETE FROM {table} {where}'),
            Query::CREATE_TABLE  => new Statement("CREATE {temporary} TABLE IF NOT EXISTS {table} (\n{columns}{keys}\n)"),
            Query::CREATE_INDEX  => new Statement('CREATE {type} INDEX IF NOT EXISTS {index} ON {table} ({fields})'),
            Query::DROP_TABLE    => new Statement('DROP TABLE IF EXISTS {table}'),
            Query::DROP_INDEX    => new Statement('DROP INDEX IF EXISTS {index}')
        ]);

        // SQLite doesn't support TRUNCATE
        unset($this->_statements[Query::TRUNCATE]);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Titon\Db\Exception\UnsupportedQueryStatementException
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
            $dataType = $this->getDriver()->getType($type);
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