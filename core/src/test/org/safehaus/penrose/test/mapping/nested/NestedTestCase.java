package org.safehaus.penrose.test.mapping.nested;

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

    public String baseDn = "ou=Groups,dc=Example,dc=com";

    public Penrose penrose;
    public PenroseConfig penroseConfig;

    public NestedTestCase() throws Exception {
    }

    public void setUp() throws Exception {

        executeUpdate("create table groups ("+
                "groupname varchar(10), "+
                "description varchar(30), "+
                "primary key (groupname))"
        );

        executeUpdate("create table members ("+
                "username varchar(10), "+
                "groupname varchar(10), "+
                "name varchar(30), "+
                "primary key (username, groupname))"
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

        SourceConfig groupsSource = new SourceConfig();
        groupsSource.setName("groups");
        groupsSource.setConnectionName("HSQLDB");
        groupsSource.setParameter("table", "groups");
        groupsSource.addFieldConfig(new FieldConfig("groupname", "GROUPNAME", true));
        groupsSource.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partitionConfig.getSourceConfigs().addSourceConfig(groupsSource);

        SourceConfig usergroupsSource = new SourceConfig();
        usergroupsSource.setName("members");
        usergroupsSource.setConnectionName("HSQLDB");
        usergroupsSource.setParameter("table", "members");
        usergroupsSource.addFieldConfig(new FieldConfig("username", "USERNAME", true));
        usergroupsSource.addFieldConfig(new FieldConfig("groupname", "GROUPNAME", true));
        usergroupsSource.addFieldConfig(new FieldConfig("name", "NAME", false));
        partitionConfig.getSourceConfigs().addSourceConfig(usergroupsSource);

        EntryMapping ou = new EntryMapping(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Groups", true));
        partitionConfig.getDirectoryConfigs().addEntryMapping(ou);

        EntryMapping groups = new EntryMapping("cn=...,"+baseDn);
        groups.addObjectClass("groupOfUniqueNames");
        groups.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "g.groupname", true));
        groups.addAttributeMapping(new AttributeMapping("description", AttributeMapping.VARIABLE, "g.description"));

        SourceMapping groupsMapping = new SourceMapping();
        groupsMapping.setName("g");
        groupsMapping.setSourceName("groups");
        groupsMapping.addFieldMapping(new FieldMapping("groupname", FieldMapping.VARIABLE, "cn"));
        groupsMapping.addFieldMapping(new FieldMapping("description", FieldMapping.VARIABLE, "description"));
        groups.addSourceMapping(groupsMapping);

        partitionConfig.getDirectoryConfigs().addEntryMapping(groups);

        EntryMapping members = new EntryMapping("uid=...,cn=...,"+baseDn);
        members.addObjectClass("person");
        members.addObjectClass("organizationalPerson");
        members.addObjectClass("inetOrgPerson");
        members.addAttributeMapping(new AttributeMapping("uid", AttributeMapping.VARIABLE, "m.username", true));
        members.addAttributeMapping(new AttributeMapping("memberOf", AttributeMapping.VARIABLE, "m.groupname"));
        members.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "m.name"));

        SourceMapping membersMapping = new SourceMapping();
        membersMapping.setName("m");
        membersMapping.setSourceName("members");
        membersMapping.addFieldMapping(new FieldMapping("username", FieldMapping.VARIABLE, "uid"));
        membersMapping.addFieldMapping(new FieldMapping("groupname", FieldMapping.VARIABLE, "g.groupname"));
        membersMapping.addFieldMapping(new FieldMapping("name", FieldMapping.VARIABLE, "cn"));
        members.addSourceMapping(membersMapping);

        partitionConfig.getDirectoryConfigs().addEntryMapping(members);

        PenroseContext penroseContext = penrose.getPenroseContext();
        Partitions partitions = penrose.getPartitions();

        PartitionContext partitionContext = new PartitionContext();
        partitionContext.setPenroseConfig(penroseConfig);
        partitionContext.setPenroseContext(penroseContext);

        Partition partition = new Partition();
        partition.init(partitionConfig, partitionContext);
        partitions.addPartition(partition);
    }
    
    public void testDummy()
    {
    	assertEquals("a", "a");
    }

    public void tearDown() throws Exception {
        penrose.stop();

        executeUpdate("drop table groups");
        executeUpdate("drop table members");
    }
}
