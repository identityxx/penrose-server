/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.partition;

import junit.framework.TestCase;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.apache.log4j.*;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PartitionTest extends TestCase {

    Partition partition;

    public PartitionTest() {
        PatternLayout patternLayout = new PatternLayout("%-20C{1} [%4L] %m%n");
        ConsoleAppender appender = new ConsoleAppender(patternLayout);
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.DEBUG);
    }

    public void setUp() throws Exception {
        PartitionReader partitionReader = new PartitionReader();
        partition = partitionReader.read("samples/conf");
/*
        EntryMapping rootEntry = new EntryMapping("dc=Example,dc=com");
        rootEntry.addAttributeMapping(new AttributeMapping("dc", AttributeMapping.CONSTANT, "Example", true));
        partition.addEntryMapping(rootEntry);
*/
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

    public void testAddingEntry() throws Exception {
        partition.addEntryMapping(new EntryMapping("cn=Groups,dc=Example,dc=com"));
        print(partition);
    }

    public void testFindingRootEntry() throws Exception {
        Collection entryMappings = partition.getEntryMappings("dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());
        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        System.out.println("Found "+entryMapping.getDn());
    }

    public void testFindingStaticEntry() throws Exception {
        Collection entryMappings = partition.getEntryMappings("cn=Users,dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());
        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        System.out.println("Found "+entryMapping.getDn());
    }

    public void testFindingDynamicEntry() throws Exception {
        Collection entryMappings = partition.getEntryMappings("cn=...,cn=Users,dc=Example,dc=com");
        assertNotNull(entryMappings);
        assertFalse(entryMappings.isEmpty());
        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        System.out.println("Found "+entryMapping.getDn());
    }

    public void print(Partition partition) throws Exception {

        System.out.println("Entries:");
        Collection c = partition.getRootEntryMappings();
        print(partition, c, 0);
    }

    public void print(Partition partition, Collection entryMappings, int level) throws Exception {

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            for (int l=0; l<level; l++) System.out.print("  ");
            System.out.println(" - "+entryMapping.getRdn());

            Collection children = partition.getChildren(entryMapping);
            print(partition, children, level+1);
        }
    }
}
