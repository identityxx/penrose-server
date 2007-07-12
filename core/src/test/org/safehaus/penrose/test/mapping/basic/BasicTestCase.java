package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.test.jdbc.JDBCTestCase;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.adapter.jdbc.JDBCAdapter;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.simple.SimpleEngine;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.partition.*;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class BasicTestCase extends JDBCTestCase {

    public Logger log = Logger.getLogger(getClass());

    public String baseDn = "ou=Groups,dc=Example,dc=com";

    public PenroseConfig penroseConfig;
    public Penrose penrose;

    public BasicTestCase() throws Exception {
    }

    public void setUp() throws Exception {

        executeUpdate("create table groups ("+
                "groupname varchar(10), "+
                "description varchar(30), "+
                "primary key (groupname))"
        );

        penroseConfig = new PenroseConfig();
        penroseConfig.addAdapterConfig(new AdapterConfig("JDBC", JDBCAdapter.class.getName()));
        penroseConfig.addEngineConfig(new EngineConfig("DEFAULT", SimpleEngine.class.getName()));

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        PenroseContext penroseContext = penrose.getPenroseContext();
        PartitionManager partitionManager = penroseContext.getPartitionManager();

        PartitionConfig partitionConfig = new PartitionConfig("DEFAULT", "conf");
        Partition partition = new Partition(partitionConfig);

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setAdapterName("JDBC");
        connectionConfig.setName("HSQLDB");
        connectionConfig.setParameter("driver", getDriver());
        connectionConfig.setParameter("url", getUrl());
        connectionConfig.setParameter("user", getUser());
        connectionConfig.setParameter("password", getPassword());
        partition.getConnections().addConnectionConfig(connectionConfig);

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setName("groups");
        sourceConfig.setConnectionName("HSQLDB");
        sourceConfig.setParameter("table", "groups");
        sourceConfig.addFieldConfig(new FieldConfig("groupname", "GROUPNAME", true));
        sourceConfig.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partition.getSources().addSourceConfig(sourceConfig);

        EntryMapping ou = new EntryMapping(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Groups", true));
        partition.getMappings().addEntryMapping(ou);

        EntryMapping groups = new EntryMapping("cn=...,"+baseDn);
        groups.addObjectClass("groupOfUniqueNames");
        groups.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "g.groupname", true));
        groups.addAttributeMapping(new AttributeMapping("description", AttributeMapping.VARIABLE, "g.description"));

        SourceMapping sourceMapping = new SourceMapping();
        sourceMapping.setName("g");
        sourceMapping.setSourceName("groups");
        sourceMapping.addFieldMapping(new FieldMapping("groupname", FieldMapping.VARIABLE, "cn"));
        sourceMapping.addFieldMapping(new FieldMapping("description", FieldMapping.VARIABLE, "description"));
        groups.addSourceMapping(sourceMapping);

        partition.getMappings().addEntryMapping(groups);

        partitionManager.addPartition(partition);

        penrose.start();
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
