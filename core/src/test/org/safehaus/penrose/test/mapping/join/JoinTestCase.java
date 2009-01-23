package org.safehaus.penrose.test.mapping.join;

import org.safehaus.penrose.test.jdbc.JDBCTestCase;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntryAttributeConfig;
import org.safehaus.penrose.directory.EntryFieldConfig;
import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;

/**
 * @author Endi S. Dewata
 */
public class JoinTestCase extends JDBCTestCase {

    public Penrose penrose;
    public PenroseConfig penroseConfig;

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

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose();
        penrose.start();

        penroseConfig = penrose.getPenroseConfig();

        PartitionConfig partitionConfig = new PartitionConfig();
        partitionConfig.setName("DEFAULT");

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setAdapterName("JDBC");
        connectionConfig.setName("HSQLDB");
        connectionConfig.setParameter("driver", driver);
        connectionConfig.setParameter("url", url);
        connectionConfig.setParameter("user", user);
        connectionConfig.setParameter("password", password);
        partitionConfig.getConnectionConfigManager().addConnectionConfig(connectionConfig);

        SourceConfig groupsSource = new SourceConfig();
        groupsSource.setName("groups");
        groupsSource.setConnectionName("HSQLDB");
        groupsSource.setParameter("table", "groups");
        groupsSource.addFieldConfig(new FieldConfig("groupname", true));
        groupsSource.addFieldConfig(new FieldConfig("description"));
        partitionConfig.getSourceConfigManager().addSourceConfig(groupsSource);

        SourceConfig usergroupsSource = new SourceConfig();
        usergroupsSource.setName("usergroups");
        usergroupsSource.setConnectionName("HSQLDB");
        usergroupsSource.setParameter("table", "usergroups");
        usergroupsSource.addFieldConfig(new FieldConfig("groupname", true));
        usergroupsSource.addFieldConfig(new FieldConfig("username", true));
        partitionConfig.getSourceConfigManager().addSourceConfig(usergroupsSource);

        EntryConfig ou = new EntryConfig("ou=Groups,dc=Example,dc=com");
        ou.addObjectClass("organizationalUnit");
        ou.addAttributesFromRdn();
        partitionConfig.getDirectoryConfig().addEntryConfig(ou);

        EntryConfig groups = new EntryConfig("cn=...,ou=Groups,dc=Example,dc=com");
        groups.addObjectClass("groupOfUniqueNames");
        groups.addAttributeConfig(new EntryAttributeConfig("cn", EntryAttributeConfig.VARIABLE, "g.groupname", true));
        groups.addAttributeConfig(new EntryAttributeConfig("description", EntryAttributeConfig.VARIABLE, "g.description"));
        groups.addAttributeConfig(new EntryAttributeConfig("uniqueMember", EntryAttributeConfig.VARIABLE, "ug.username"));

        EntrySourceConfig groupsMapping = new EntrySourceConfig();
        groupsMapping.setAlias("g");
        groupsMapping.setSourceName("groups");
        groupsMapping.addFieldConfig(new EntryFieldConfig("groupname", EntryFieldConfig.VARIABLE, "cn"));
        groupsMapping.addFieldConfig(new EntryFieldConfig("description", EntryFieldConfig.VARIABLE, "description"));
        groups.addSourceConfig(groupsMapping);

        EntrySourceConfig usergroupsMapping = new EntrySourceConfig();
        usergroupsMapping.setAlias("ug");
        usergroupsMapping.setSourceName("usergroups");
        usergroupsMapping.addFieldConfig(new EntryFieldConfig("groupname", EntryFieldConfig.VARIABLE, "g.groupname"));
        usergroupsMapping.addFieldConfig(new EntryFieldConfig("username", EntryFieldConfig.VARIABLE, "uniqueMember"));
        usergroupsMapping.setSearch(EntrySourceConfig.REQUIRED);
        groups.addSourceConfig(usergroupsMapping);

        partitionConfig.getDirectoryConfig().addEntryConfig(groups);

        PartitionManager partitionManager = penrose.getPartitionManager();
        partitionManager.createPartition(partitionConfig);
    }

    public void tearDown() throws Exception {
        penrose.stop();

        executeUpdate("drop table groups");
        executeUpdate("drop table usergroups");
    }
}
