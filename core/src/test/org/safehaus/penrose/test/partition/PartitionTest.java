package org.safehaus.penrose.test.partition;

import junit.framework.TestCase;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntryAttributeConfig;
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

        EntryConfig rootEntry = new EntryConfig("dc=Example,dc=com");
        rootEntry.addAttributesFromRdn();
        partitionConfig.getDirectoryConfig().addEntryConfig(rootEntry);

        EntryConfig usersEntry = new EntryConfig("cn=Users,dc=Example,dc=com");
        usersEntry.addAttributesFromRdn();
        partitionConfig.getDirectoryConfig().addEntryConfig(usersEntry);

        EntryConfig users1Mapping = new EntryConfig("cn=...,cn=Users,dc=Example,dc=com");
        users1Mapping.addAttributeConfig(new EntryAttributeConfig("cn", EntryAttributeConfig.VARIABLE, "users.cn", true));
        partitionConfig.getDirectoryConfig().addEntryConfig(users1Mapping);

        EntryConfig users2Mapping = new EntryConfig("cn=...,cn=Users,dc=Example,dc=com");
        users2Mapping.addAttributeConfig(new EntryAttributeConfig("cn", EntryAttributeConfig.VARIABLE, "groups.cn", true));
        partitionConfig.getDirectoryConfig().addEntryConfig(users2Mapping);
    }

    public void tearDown() throws Exception {
    }
/*
    public void testAddingEntry() throws Exception {
        partition.addEntryConfig(new EntryConfig("cn=Groups,dc=Example,dc=com"));
        print(partition);
    }
*/
    public void testFindingRootEntry() throws Exception {
        Collection<EntryConfig> entryConfigs = partitionConfig.getDirectoryConfig().getEntryConfigs("dc=Example,dc=com");
        assertNotNull(entryConfigs);
        assertFalse(entryConfigs.isEmpty());

        EntryConfig entryConfig = (EntryConfig)entryConfigs.iterator().next();
        assertTrue(entryConfig.getDn().matches("dc=Example,dc=com"));
    }

    public void testFindingStaticEntry() throws Exception {
        Collection<EntryConfig> entryConfigs = partitionConfig.getDirectoryConfig().getEntryConfigs("cn=Users,dc=Example,dc=com");
        assertNotNull(entryConfigs);
        assertFalse(entryConfigs.isEmpty());

        EntryConfig entryConfig = (EntryConfig)entryConfigs.iterator().next();
        assertTrue(entryConfig.getDn().matches("cn=Users,dc=Example,dc=com"));
    }

    public void testFindingDynamicEntry() throws Exception {
        Collection<EntryConfig> entryConfigs = partitionConfig.getDirectoryConfig().getEntryConfigs("cn=...,cn=Users,dc=Example,dc=com");
        assertNotNull(entryConfigs);
        assertFalse(entryConfigs.isEmpty());

        EntryConfig entryConfig = (EntryConfig)entryConfigs.iterator().next();
        assertTrue(entryConfig.getDn().matches("cn=...,cn=Users,dc=Example,dc=com"));
    }

    public void print(Partition partition) throws Exception {
        log.debug("Entries:");
        PartitionConfig partitionConfig = partition.getPartitionConfig();
        Collection<EntryConfig> c = partitionConfig.getDirectoryConfig().getRootEntryConfigs();
        print(partition, c, 0);
    }

    public void print(Partition partition, Collection<EntryConfig> entryConfigs, int level) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();

        for (EntryConfig entryConfig : entryConfigs) {

            for (int l = 0; l < level; l++) System.out.print("  ");
            log.debug(" - " + entryConfig.getRdn());

            Collection<EntryConfig> children = partitionConfig.getDirectoryConfig().getChildren(entryConfig);
            print(partition, children, level + 1);
        }
    }
}
