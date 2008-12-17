package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.test.jdbc.JDBCTestCase;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntryAttributeConfig;
import org.safehaus.penrose.directory.EntryFieldConfig;
import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.partition.*;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class BasicTestCase extends JDBCTestCase {

    public Logger log = Logger.getLogger(getClass());

    public String baseDn = "ou=Groups,dc=Example,dc=com";

    public Penrose penrose;
    public PenroseConfig penroseConfig;

    public BasicTestCase() throws Exception {
    }

    public void setUp() throws Exception {

        executeUpdate("create table groups ("+
                "groupname varchar(10), "+
                "description varchar(30), "+
                "primary key (groupname))"
        );

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose();
        penrose.start();

        penroseConfig = penrose.getPenroseConfig();

        PartitionConfig partitionConfig = new PartitionConfig("DEFAULT");

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setAdapterName("JDBC");
        connectionConfig.setName("HSQLDB");
        connectionConfig.setParameter("driver", getDriver());
        connectionConfig.setParameter("url", getUrl());
        connectionConfig.setParameter("user", getUser());
        connectionConfig.setParameter("password", getPassword());
        partitionConfig.getConnectionConfigManager().addConnectionConfig(connectionConfig);

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setName("groups");
        sourceConfig.setConnectionName("HSQLDB");
        sourceConfig.setParameter("table", "groups");
        sourceConfig.addFieldConfig(new FieldConfig("groupname", "GROUPNAME", true));
        sourceConfig.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partitionConfig.getSourceConfigManager().addSourceConfig(sourceConfig);

        EntryConfig ou = new EntryConfig(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributesFromRdn();
        partitionConfig.getDirectoryConfig().addEntryConfig(ou);

        EntryConfig groups = new EntryConfig("cn=...,"+baseDn);
        groups.addObjectClass("groupOfUniqueNames");
        groups.addAttributeConfig(new EntryAttributeConfig("cn", EntryAttributeConfig.VARIABLE, "g.groupname", true));
        groups.addAttributeConfig(new EntryAttributeConfig("description", EntryAttributeConfig.VARIABLE, "g.description"));

        EntrySourceConfig sourceMapping = new EntrySourceConfig();
        sourceMapping.setAlias("g");
        sourceMapping.setSourceName("groups");
        sourceMapping.addFieldConfig(new EntryFieldConfig("groupname", EntryFieldConfig.VARIABLE, "cn"));
        sourceMapping.addFieldConfig(new EntryFieldConfig("description", EntryFieldConfig.VARIABLE, "description"));
        groups.addSourceConfig(sourceMapping);

        partitionConfig.getDirectoryConfig().addEntryConfig(groups);

        PartitionManager partitionManager = penrose.getPartitionManager();
        partitionManager.createPartition(partitionConfig);
    }
/*
    public void testDummy()
    {
    	assertEquals("a", "a");
    }
*/    
    public void tearDown() throws Exception {
        penrose.stop();

        executeUpdate("drop table groups");
    }
}
