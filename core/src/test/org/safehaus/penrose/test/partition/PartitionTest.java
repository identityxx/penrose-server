package org.safehaus.penrose.test.partition;

import junit.framework.TestCase;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PartitionTest extends TestCase {

    Logger log = Logger.getLogger(getClass());

    Partition partition;

    public PartitionTest() {
    }

    public void setUp() throws Exception {
        PartitionConfig partitionConfig = new PartitionConfig("example", "target/example");
        partition = new Partition(partitionConfig);

        EntryMapping rootEntry = new EntryMapping("dc=Example,dc=com");
        rootEntry.addAttributeMapping(new AttributeMapping("dc", AttributeMapping.CONSTANT, "Example", true));
        partition.addEntryMapping(rootEntry);

        EntryMapping usersEntry = new EntryMapping("cn=Users,dc=Example,dc=com");
        usersEntry.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.CONSTANT, "Users", true));
        partition.addEntryMapping(usersEntry);

        EntryMapping users1Mapping = new EntryMapping("cn=...,cn=Users,dc=Example,dc=com");
        users1Mapping.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "users.cn", true));
        partition.addEntryMapping(users1Mapping);

        EntryMapping users2Mapping = new EntryMapping("cn=...,cn=Users,dc=Example,dc=com");
        users2Mapping.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "groups.cn", true));
        partition.addEntryMapping(users2Mapping);

        EntryMapping proxyMapping = new EntryMapping("cn=Proxy,dc=Example,dc=com");
        proxyMapping.addSourceMapping(new SourceMapping("DEFAULT", "source"));
        partition.addEntryMapping(proxyMapping);
    }

    public void tearDown() throws Exception {
    }
/*
    public void testAddingEntry() throws Exception {
        partition.addEntryMapping(new EntryMapping("cn=Groups,dc=Example,dc=com"));
        print(partition);
    }
*/
    public void testFindingRootEntry() throws Exception {
        Collection entryMappings = partition.getEntryMappings("dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());

        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        assertTrue(entryMapping.getDn().matches("dc=Example,dc=com"));
    }

    public void testFindingStaticEntry() throws Exception {
        Collection entryMappings = partition.getEntryMappings("cn=Users,dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());

        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        assertTrue(entryMapping.getDn().matches("cn=Users,dc=Example,dc=com"));
    }

    public void testFindingDynamicEntry() throws Exception {
        Collection entryMappings = partition.getEntryMappings("cn=...,cn=Users,dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());

        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        assertTrue(entryMapping.getDn().matches("cn=...,cn=Users,dc=Example,dc=com"));
    }

    public void print(Partition partition) throws Exception {
        log.debug("Entries:");
        Collection c = partition.getRootEntryMappings();
        print(partition, c, 0);
    }

    public void print(Partition partition, Collection entryMappings, int level) throws Exception {

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            for (int l=0; l<level; l++) System.out.print("  ");
            log.debug(" - "+entryMapping.getRdn());

            Collection children = partition.getChildren(entryMapping);
            print(partition, children, level+1);
        }
    }
}
