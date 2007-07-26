package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.simple.SimpleEngine;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
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
        penroseConfig.addEngineConfig(new EngineConfig("DEFAULT", SimpleEngine.class.getName()));

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        PenroseContext penroseContext = penrose.getPenroseContext();
        PartitionManager partitionManager = penroseContext.getPartitionManager();

        PartitionConfig partitionConfig = new PartitionConfig("DEFAULT");
        Partition partition = new Partition(partitionConfig);

        EntryMapping ou = new EntryMapping(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Groups", true));
        partition.getMappings().addEntryMapping(ou);

        EntryMapping group = new EntryMapping("cn=group,"+baseDn);
        group.addObjectClass("groupOfUniqueNames");
        group.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.CONSTANT, "group", true));
        group.addAttributeMapping(new AttributeMapping("description", AttributeMapping.CONSTANT, "description"));
        group.addAttributeMapping(new AttributeMapping("uniqueMember", AttributeMapping.CONSTANT, "member1"));
        group.addAttributeMapping(new AttributeMapping("uniqueMember", AttributeMapping.CONSTANT, "member2"));
        group.addAttributeMapping(new AttributeMapping("creatorsName", AttributeMapping.CONSTANT, penroseConfig.getRootDn().toString()));
        
        partition.getMappings().addEntryMapping(group);

        EntryMapping member1 = new EntryMapping("uid=member1,cn=group,"+baseDn);
        member1.addObjectClass("person");
        member1.addObjectClass("organizationalPerson");
        member1.addObjectClass("inetOrgPerson");
        member1.addAttributeMapping(new AttributeMapping("uid", AttributeMapping.CONSTANT, "member1", true));
        member1.addAttributeMapping(new AttributeMapping("memberOf", AttributeMapping.CONSTANT, "group"));

        partition.getMappings().addEntryMapping(member1);
        
        EntryMapping member2 = new EntryMapping("uid=member2,cn=group,"+baseDn);
        member2.addObjectClass("person");
        member2.addObjectClass("organizationalPerson");
        member2.addObjectClass("inetOrgPerson");
        member2.addAttributeMapping(new AttributeMapping("uid", AttributeMapping.CONSTANT, "member2", true));
        member2.addAttributeMapping(new AttributeMapping("memberOf", AttributeMapping.CONSTANT, "group"));

        partition.getMappings().addEntryMapping(member2);

        partitionManager.addPartition(partition);

        penrose.start();
    }
    
    public void testDummy()
    {
    	assertEquals("a", "a");
    }

    public void tearDown() throws Exception {
        penrose.stop();
    }
}
