<?php
namespace Titon\Db\Sqlite\Behavior;

class HierarchyBehaviorTest extends \Titon\Db\Behavior\HierarchyBehaviorTest {

    public function testGetTreeNoRecords() {
        $this->object->dropTable();
        $this->object->createTable(); // SQLite doesn't support TRUNCATE

        $this->assertEquals([], $this->object->Hierarchy->getTree());
    }

}