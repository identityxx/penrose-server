package org.safehaus.penrose.test.partition;

import junit.framework.TestCase;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.directory.EntryMapping;
import org.safehaus.penrose.directory.AttributeMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PartitionTest extends TestCase {

    Logger log = Logger.getLogger(getClass());

    PartitionConfig partitionConfig;

    public PartitionTest() {
    }

    public void setUp() throws Exception {
        partitionConfig = new PartitionConfig("example");

        EntryMapping rootEntry = new EntryMapping("dc=Example,dc=com");
        rootEntry.addAttributeMapping(new AttributeMapping("dc", AttributeMapping.CONSTANT, "Example", true));
        partitionConfig.getDirectoryConfig().addEntryMapping(rootEntry);

        EntryMapping usersEntry = new EntryMapping("cn=Users,dc=Example,dc=com");
        usersEntry.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.CONSTANT, "Users", true));
        partitionConfig.getDirectoryConfig().addEntryMapping(usersEntry);

        EntryMapping users1Mapping = new EntryMapping("cn=...,cn=Users,dc=Example,dc=com");
        users1Mapping.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "users.cn", true));
        partitionConfig.getDirectoryConfig().addEntryMapping(users1Mapping);

        EntryMapping users2Mapping = new EntryMapping("cn=...,cn=Users,dc=Example,dc=com");
        users2Mapping.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "groups.cn", true));
        partitionConfig.getDirectoryConfig().addEntryMapping(users2Mapping);

        EntryMapping proxyMapping = new EntryMapping("cn=Proxy,dc=Example,dc=com");
        proxyMapping.addSourceMapping(new SourceMapping("DEFAULT", "source"));
        partitionConfig.getDirectoryConfig().addEntryMapping(proxyMapping);
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
        Collection entryMappings = partitionConfig.getDirectoryConfig().getEntryMappings("dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());

        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        assertTrue(entryMapping.getDn().matches("dc=Example,dc=com"));
    }

    public void testFindingStaticEntry() throws Exception {
        Collection entryMappings = partitionConfig.getDirectoryConfig().getEntryMappings("cn=Users,dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());

        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        assertTrue(entryMapping.getDn().matches("cn=Users,dc=Example,dc=com"));
    }

    public void testFindingDynamicEntry() throws Exception {
        Collection entryMappings = partitionConfig.getDirectoryConfig().getEntryMappings("cn=...,cn=Users,dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());

        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        assertTrue(entryMapping.getDn().matches("cn=...,cn=Users,dc=Example,dc=com"));
    }

    public void print(Partition partition) throws Exception {
        log.debug("Entries:");
        PartitionConfig partitionConfig = partition.getPartitionConfig();
        Collection<EntryMapping> c = partitionConfig.getDirectoryConfig().getRootEntryMappings();
        print(partition, c, 0);
    }

    public void print(Partition partition, Collection<EntryMapping> entryMappings, int level) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();

        for (EntryMapping entryMapping : entryMappings) {

            for (int l = 0; l < level; l++) System.out.print("  ");
            log.debug(" - " + entryMapping.getRdn());

            Collection<EntryMapping> children = partitionConfig.getDirectoryConfig().getChildren(entryMapping);
            print(partition, children, level + 1);
        }
    }
}
