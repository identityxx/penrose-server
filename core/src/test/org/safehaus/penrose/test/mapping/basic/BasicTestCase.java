package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.test.jdbc.JDBCTestCase;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.PenroseConfig;
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

        PenroseContext penroseContext = penrose.getPenroseContext();
        Partitions partitions = penrose.getPartitions();

        PartitionConfig partitionConfig = new PartitionConfig("DEFAULT");

        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setAdapterName("JDBC");
        connectionConfig.setName("HSQLDB");
        connectionConfig.setParameter("driver", getDriver());
        connectionConfig.setParameter("url", getUrl());
        connectionConfig.setParameter("user", getUser());
        connectionConfig.setParameter("password", getPassword());
        partitionConfig.getConnectionConfigs().addConnectionConfig(connectionConfig);

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setName("groups");
        sourceConfig.setConnectionName("HSQLDB");
        sourceConfig.setParameter("table", "groups");
        sourceConfig.addFieldConfig(new FieldConfig("groupname", "GROUPNAME", true));
        sourceConfig.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partitionConfig.getSourceConfigs().addSourceConfig(sourceConfig);

        EntryMapping ou = new EntryMapping(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Groups", true));
        partitionConfig.getDirectoryConfigs().addEntryMapping(ou);

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

        partitionConfig.getDirectoryConfigs().addEntryMapping(groups);

        partitions.init(penroseConfig, penroseContext, partitionConfig);
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
