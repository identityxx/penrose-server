package org.safehaus.penrose.test.mapping.join;

import org.safehaus.penrose.test.jdbc.JDBCTestCase;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.adapter.jdbc.JDBCAdapter;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.DefaultEngine;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.naming.PenroseContext;

/**
 * @author Endi S. Dewata
 */
public class JoinTestCase extends JDBCTestCase {

    public Penrose penrose;

    public JoinTestCase() throws Exception {
    }

    public void setUp() throws Exception {

        executeUpdate("create table groups ("+
                "groupname varchar(10), "+
                "description varchar(30), "+
                "primary key (groupname))"
        );

        executeUpdate("create table usergroups ("+
                "groupname varchar(10), "+
                "username varchar(10), "+
                "primary key (groupname, username))"
        );

        PenroseConfig penroseConfig = new PenroseConfig();
        penroseConfig.addAdapterConfig(new AdapterConfig("JDBC", JDBCAdapter.class.getName()));
        penroseConfig.addEngineConfig(new EngineConfig("DEFAULT", DefaultEngine.class.getName()));

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
        groupsSource.addFieldConfig(new FieldConfig("groupname", true));
        groupsSource.addFieldConfig(new FieldConfig("description"));
        partition.getSources().addSourceConfig(groupsSource);

        SourceConfig usergroupsSource = new SourceConfig();
        usergroupsSource.setName("usergroups");
        usergroupsSource.setConnectionName("HSQLDB");
        usergroupsSource.setParameter("table", "usergroups");
        usergroupsSource.addFieldConfig(new FieldConfig("groupname", true));
        usergroupsSource.addFieldConfig(new FieldConfig("username", true));
        partition.getSources().addSourceConfig(usergroupsSource);

        EntryMapping ou = new EntryMapping("ou=Groups,dc=Example,dc=com");
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Groups", true));
        partition.addEntryMapping(ou);

        EntryMapping groups = new EntryMapping("cn=...,ou=Groups,dc=Example,dc=com");
        groups.addObjectClass("groupOfUniqueNames");
        groups.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "g.groupname", true));
        groups.addAttributeMapping(new AttributeMapping("description", AttributeMapping.VARIABLE, "g.description"));
        groups.addAttributeMapping(new AttributeMapping("uniqueMember", AttributeMapping.VARIABLE, "ug.username"));

        SourceMapping groupsMapping = new SourceMapping();
        groupsMapping.setName("g");
        groupsMapping.setSourceName("groups");
        groupsMapping.addFieldMapping(new FieldMapping("groupname", FieldMapping.VARIABLE, "cn"));
        groupsMapping.addFieldMapping(new FieldMapping("description", FieldMapping.VARIABLE, "description"));
        groups.addSourceMapping(groupsMapping);

        SourceMapping usergroupsMapping = new SourceMapping();
        usergroupsMapping.setName("ug");
        usergroupsMapping.setSourceName("usergroups");
        usergroupsMapping.addFieldMapping(new FieldMapping("groupname", FieldMapping.VARIABLE, "g.groupname"));
        usergroupsMapping.addFieldMapping(new FieldMapping("username", FieldMapping.VARIABLE, "uniqueMember"));
        usergroupsMapping.setSearch(SourceMapping.REQUIRED);
        groups.addSourceMapping(usergroupsMapping);

        partition.addEntryMapping(groups);

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);

        PenroseContext penroseContext = penrose.getPenroseContext();
        PartitionManager partitionManager = penroseContext.getPartitionManager();
        partitionManager.addPartition(partition);

        penrose.start();
    }

    public void tearDown() throws Exception {
        penrose.stop();

        executeUpdate("drop table groups");
        executeUpdate("drop table usergroups");
    }
}
