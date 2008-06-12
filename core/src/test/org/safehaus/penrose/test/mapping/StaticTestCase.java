package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.AttributeMapping;
import org.safehaus.penrose.naming.PenroseContext;
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

        PenroseContext penroseContext = penrose.getPenroseContext();
        PartitionManager partitionManager = penrose.getPartitionManager();

        PartitionConfig partitionConfig = new PartitionConfig("DEFAULT");

        EntryConfig ou = new EntryConfig(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMappingsFromRdn();
        partitionConfig.getDirectoryConfig().addEntryConfig(ou);

        EntryConfig group = new EntryConfig("cn=group,"+baseDn);
        group.addObjectClass("groupOfUniqueNames");
        group.addAttributeMappingsFromRdn();
        group.addAttributeMapping("description", "description");
        group.addAttributeMapping("uniqueMember", "member1");
        group.addAttributeMapping("uniqueMember", "member2");
        group.addAttributeMapping("creatorsName", penroseConfig.getRootDn().toString());
        
        partitionConfig.getDirectoryConfig().addEntryConfig(group);

        EntryConfig member1 = new EntryConfig("uid=member1,cn=group,"+baseDn);
        member1.addObjectClass("person");
        member1.addObjectClass("organizationalPerson");
        member1.addObjectClass("inetOrgPerson");
        member1.addAttributeMappingsFromRdn();
        member1.addAttributeMapping("memberOf", "group");

        partitionConfig.getDirectoryConfig().addEntryConfig(member1);
        
        EntryConfig member2 = new EntryConfig("uid=member2,cn=group,"+baseDn);
        member2.addObjectClass("person");
        member2.addObjectClass("organizationalPerson");
        member2.addObjectClass("inetOrgPerson");
        member2.addAttributeMappingsFromRdn();
        member2.addAttributeMapping("memberOf", "group");

        partitionConfig.getDirectoryConfig().addEntryConfig(member2);

        PartitionFactory partitionFactory = new PartitionFactory();
        partitionFactory.setPenroseConfig(penroseConfig);
        partitionFactory.setPenroseContext(penroseContext);

        Partition partition = partitionFactory.createPartition(partitionConfig);

        partitionManager.addPartition(partition);
    }
    
    public void testDummy()
    {
    	assertEquals("a", "a");
    }

    public void tearDown() throws Exception {
        penrose.stop();
    }
}
