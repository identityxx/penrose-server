package org.safehaus.penrose.test.mapping.nested;

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

    public String baseDn = "ou=Groups,dc=Example,dc=com";

    public PenroseConfig penroseConfig;
    public Penrose penrose;

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
        partition.getConnections().addConnectionConfig(connectionConfig);

        SourceConfig groupsSource = new SourceConfig();
        groupsSource.setName("groups");
        groupsSource.setConnectionName("HSQLDB");
        groupsSource.setParameter("table", "groups");
        groupsSource.addFieldConfig(new FieldConfig("groupname", "GROUPNAME", true));
        groupsSource.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partition.getSources().addSourceConfig(groupsSource);

        SourceConfig usergroupsSource = new SourceConfig();
        usergroupsSource.setName("members");
        usergroupsSource.setConnectionName("HSQLDB");
        usergroupsSource.setParameter("table", "members");
        usergroupsSource.addFieldConfig(new FieldConfig("username", "USERNAME", true));
        usergroupsSource.addFieldConfig(new FieldConfig("groupname", "GROUPNAME", true));
        usergroupsSource.addFieldConfig(new FieldConfig("name", "NAME", false));
        partition.getSources().addSourceConfig(usergroupsSource);

        EntryMapping ou = new EntryMapping(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Groups", true));
        partition.addEntryMapping(ou);

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

        partition.addEntryMapping(groups);

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

        executeUpdate("drop table groups");
        executeUpdate("drop table members");
    }
}
