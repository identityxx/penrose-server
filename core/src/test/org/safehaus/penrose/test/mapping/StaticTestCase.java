package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.partition.*;
import org.apache.log4j.Logger;
import junit.framework.TestCase;

/**
 * @author Endi S. Dewata
 */
public class StaticTestCase extends TestCase {

    public Logger log = Logger.getLogger(getClass());

    public PenroseConfig penroseConfig;
    public Penrose penrose;

    public String baseDn = "ou=Groups,dc=Example,dc=com";

    public StaticTestCase() throws Exception {
    }

    public void setUp() throws Exception {

        penroseConfig = new DefaultPenroseConfig();

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        penrose.start();

        PartitionConfig partitionConfig = new PartitionConfig();
        partitionConfig.setName(PartitionConfig.ROOT);

        EntryConfig ou = new EntryConfig(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributesFromRdn();
        partitionConfig.getDirectoryConfig().addEntryConfig(ou);

        EntryConfig group = new EntryConfig("cn=group,"+baseDn);
        group.addObjectClass("groupOfUniqueNames");
        group.addAttributesFromRdn();
        group.addAttributeConfig("description", "description");
        group.addAttributeConfig("uniqueMember", "member1");
        group.addAttributeConfig("uniqueMember", "member2");
        group.addAttributeConfig("creatorsName", penroseConfig.getRootDn().toString());
        
        partitionConfig.getDirectoryConfig().addEntryConfig(group);

        EntryConfig member1 = new EntryConfig("uid=member1,cn=group,"+baseDn);
        member1.addObjectClass("person");
        member1.addObjectClass("organizationalPerson");
        member1.addObjectClass("inetOrgPerson");
        member1.addAttributesFromRdn();
        member1.addAttributeConfig("memberOf", "group");

        partitionConfig.getDirectoryConfig().addEntryConfig(member1);
        
        EntryConfig member2 = new EntryConfig("uid=member2,cn=group,"+baseDn);
        member2.addObjectClass("person");
        member2.addObjectClass("organizationalPerson");
        member2.addObjectClass("inetOrgPerson");
        member2.addAttributesFromRdn();
        member2.addAttributeConfig("memberOf", "group");

        partitionConfig.getDirectoryConfig().addEntryConfig(member2);

        PartitionManager partitionManager = penrose.getPartitionManager();
        partitionManager.createPartition(partitionConfig);
    }
    
    public void testDummy()
    {
    	assertEquals("a", "a");
    }

    public void tearDown() throws Exception {
        penrose.stop();
    }
}
