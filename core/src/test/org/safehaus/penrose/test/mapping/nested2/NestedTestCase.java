package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.test.jdbc.JDBCTestCase;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.naming.PenroseContext;

/**
 * @author Endi S. Dewata
 */
public class NestedTestCase extends JDBCTestCase {

    public String baseDn = "ou=Parents,dc=Example,dc=com";

    public Penrose penrose;
    public PenroseConfig penroseConfig;

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


        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose();
        penrose.start();

        penroseConfig = penrose.getPenroseConfig();
        
        PartitionConfig partitionConfig = new PartitionConfig("DEFAULT");

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setAdapterName("JDBC");
        connectionConfig.setName("HSQLDB");
        connectionConfig.setParameter("driver", driver);
        connectionConfig.setParameter("url", url);
        connectionConfig.setParameter("user", user);
        connectionConfig.setParameter("password", password);
        partitionConfig.getConnectionConfigs().addConnectionConfig(connectionConfig);

        SourceConfig parentsSource = new SourceConfig();
        parentsSource.setName("parents");
        parentsSource.setConnectionName("HSQLDB");
        parentsSource.setParameter("table", "parents");
        parentsSource.addFieldConfig(new FieldConfig("parentname", "PARENTNAME", true));
        parentsSource.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partitionConfig.getSourceConfigs().addSourceConfig(parentsSource);

        SourceConfig childrenSource = new SourceConfig();
        childrenSource.setName("children");
        childrenSource.setConnectionName("HSQLDB");
        childrenSource.setParameter("table", "children");
        childrenSource.addFieldConfig(new FieldConfig("parentname", "PARENTNAME", true));
        childrenSource.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partitionConfig.getSourceConfigs().addSourceConfig(childrenSource);

        EntryMapping ou = new EntryMapping(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Parents", true));
        partitionConfig.getDirectoryConfigs().addEntryMapping(ou);

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

        partitionConfig.getDirectoryConfigs().addEntryMapping(groups);

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

        partitionConfig.getDirectoryConfigs().addEntryMapping(members);

        PenroseContext penroseContext = penrose.getPenroseContext();
        Partitions partitions = penrose.getPartitions();

        PartitionContext partitionContext = new PartitionContext();
        partitionContext.setPenroseConfig(penroseConfig);
        partitionContext.setPenroseContext(penroseContext);

        partitions.init(partitionConfig, partitionContext);
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
