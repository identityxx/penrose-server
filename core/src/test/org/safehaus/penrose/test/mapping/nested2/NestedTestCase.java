package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.test.jdbc.JDBCTestCase;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.adapter.jdbc.JDBCAdapter;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.simple.SimpleEngine;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.naming.PenroseContext;

/**
 * @author Endi S. Dewata
 */
public class NestedTestCase extends JDBCTestCase {

    public String baseDn = "ou=Parents,dc=Example,dc=com";

    public PenroseConfig penroseConfig;
    public Penrose penrose;

    public NestedTestCase() throws Exception {
    }

    public void setUp() throws Exception {

        executeUpdate("create table parents ("+
                "parentname varchar(10), "+
                "description varchar(30), "+
                "primary key (parentname))"
        );

        executeUpdate("create table children ("+
                "parentname varchar(10), "+
                "description varchar(30), "+
                "primary key (parentname))"
        );

        penroseConfig = new PenroseConfig();
        penroseConfig.addAdapterConfig(new AdapterConfig("JDBC", JDBCAdapter.class.getName()));
        penroseConfig.addEngineConfig(new EngineConfig("DEFAULT", SimpleEngine.class.getName()));

        PartitionConfig partitionConfig = new PartitionConfig("DEFAULT", "conf");
        Partition partition = new Partition(partitionConfig);

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setAdapterName("JDBC");
        connectionConfig.setName("HSQLDB");
        connectionConfig.setParameter("driver", driver);
        connectionConfig.setParameter("url", url);
        connectionConfig.setParameter("user", user);
        connectionConfig.setParameter("password", password);
        partition.addConnectionConfig(connectionConfig);

        SourceConfig parentsSource = new SourceConfig();
        parentsSource.setName("parents");
        parentsSource.setConnectionName("HSQLDB");
        parentsSource.setParameter("table", "parents");
        parentsSource.addFieldConfig(new FieldConfig("parentname", "PARENTNAME", true));
        parentsSource.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partition.getSources().addSourceConfig(parentsSource);

        SourceConfig childrenSource = new SourceConfig();
        childrenSource.setName("children");
        childrenSource.setConnectionName("HSQLDB");
        childrenSource.setParameter("table", "children");
        childrenSource.addFieldConfig(new FieldConfig("parentname", "PARENTNAME", true));
        childrenSource.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partition.getSources().addSourceConfig(childrenSource);

        EntryMapping ou = new EntryMapping(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Parents", true));
        partition.addEntryMapping(ou);

        EntryMapping groups = new EntryMapping("cn=...,"+baseDn);
        groups.addObjectClass("groupOfUniqueNames");
        groups.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "p.parentname", true));
        groups.addAttributeMapping(new AttributeMapping("description", AttributeMapping.VARIABLE, "p.description"));

        SourceMapping groupsMapping = new SourceMapping();
        groupsMapping.setName("p");
        groupsMapping.setSourceName("parents");
        groupsMapping.addFieldMapping(new FieldMapping("parentname", FieldMapping.VARIABLE, "cn"));
        groupsMapping.addFieldMapping(new FieldMapping("description", FieldMapping.VARIABLE, "description"));
        groups.addSourceMapping(groupsMapping);

        partition.addEntryMapping(groups);

        EntryMapping members = new EntryMapping("uid=child,cn=...,"+baseDn);
        members.addObjectClass("person");
        members.addObjectClass("organizationalPerson");
        members.addObjectClass("inetOrgPerson");
        members.addAttributeMapping(new AttributeMapping("uid", AttributeMapping.CONSTANT, "child", true));
        members.addAttributeMapping(new AttributeMapping("description", AttributeMapping.VARIABLE, "c.description"));

        SourceMapping membersMapping = new SourceMapping();
        membersMapping.setName("c");
        membersMapping.setSourceName("children");
        membersMapping.addFieldMapping(new FieldMapping("parentname", FieldMapping.VARIABLE, "p.parentname"));
        membersMapping.addFieldMapping(new FieldMapping("description", FieldMapping.VARIABLE, "description"));
        members.addSourceMapping(membersMapping);

        partition.addEntryMapping(members);

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);

        PenroseContext penroseContext = penrose.getPenroseContext();
        PartitionManager partitionManager = penroseContext.getPartitionManager();
        partitionManager.addPartition(partition);

        penrose.start();
    }
    
    public void testDummy()
    {
    	assertEquals("a", "a");
    }

    public void tearDown() throws Exception {
        penrose.stop();

        executeUpdate("drop table parents");
        executeUpdate("drop table children");
    }
}
