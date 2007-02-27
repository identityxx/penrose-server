package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.connector.AdapterConfig;
import org.safehaus.penrose.jdbc.JDBCAdapter;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.DefaultEngine;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.test.jdbc.JDBCTestCase;

/**
 * @author Endi S. Dewata
 */
public class BasicTestCase extends JDBCTestCase {

    public BasicTestCase() throws Exception {
    }

    public void setUp() throws Exception {
        
        executeUpdate("create table groups ("+
                "groupname varchar(10), "+
                "description varchar(30), "+
                "primary key (groupname))"
        );

        PenroseConfig penroseConfig = new PenroseConfig();
        penroseConfig.addAdapterConfig(new AdapterConfig("JDBC", JDBCAdapter.class.getName()));
        penroseConfig.addEngineConfig(new EngineConfig("DEFAULT", DefaultEngine.class.getName()));

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        PartitionManager partitionManager = penrose.getPartitionManager();

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

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setName("groups");
        sourceConfig.setConnectionName("HSQLDB");
        sourceConfig.setParameter("table", "groups");
        sourceConfig.addFieldConfig(new FieldConfig("groupname", "true"));
        sourceConfig.addFieldConfig(new FieldConfig("description"));
        partition.addSourceConfig(sourceConfig);

        EntryMapping ou = new EntryMapping("ou=Groups,dc=Example,dc=com");
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Groups", true));
        partition.addEntryMapping(ou);

        EntryMapping groups = new EntryMapping("cn=...,ou=Groups,dc=Example,dc=com");
        groups.addObjectClass("groupOfUniqueNames");
        groups.addAttributeMapping(new AttributeMapping("cn", AttributeMapping.VARIABLE, "g.groupname", true));
        groups.addAttributeMapping(new AttributeMapping("description", AttributeMapping.VARIABLE, "g.description"));

        SourceMapping sourceMapping = new SourceMapping();
        sourceMapping.setName("g");
        sourceMapping.setSourceName("groups");
        sourceMapping.addFieldMapping(new FieldMapping("groupname", FieldMapping.VARIABLE, "cn"));
        sourceMapping.addFieldMapping(new FieldMapping("description", FieldMapping.VARIABLE, "description"));
        groups.addSourceMapping(sourceMapping);

        partition.addEntryMapping(groups);

        partitionManager.addPartition(partition);

        penrose.start();
    }

    public void tearDown() throws Exception {
        penrose.stop();

        executeUpdate("drop table groups");
    }
}
