package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.test.jdbc.JDBCTestCase;
import org.safehaus.penrose.partition.*;
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

        SourceConfig parentsSource = new SourceConfig();
        parentsSource.setName("parents");
        parentsSource.setConnectionName("HSQLDB");
        parentsSource.setParameter("table", "parents");
        parentsSource.addFieldConfig(new FieldConfig("parentname", "PARENTNAME", true));
        parentsSource.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partitionConfig.getSourceConfigManager().addSourceConfig(parentsSource);

        SourceConfig childrenSource = new SourceConfig();
        childrenSource.setName("children");
        childrenSource.setConnectionName("HSQLDB");
        childrenSource.setParameter("table", "children");
        childrenSource.addFieldConfig(new FieldConfig("parentname", "PARENTNAME", true));
        childrenSource.addFieldConfig(new FieldConfig("description", "DESCRIPTION", false));
        partitionConfig.getSourceConfigManager().addSourceConfig(childrenSource);

        EntryConfig ou = new EntryConfig(baseDn);
        ou.addObjectClass("organizationalUnit");
        ou.addAttributesFromRdn();
        partitionConfig.getDirectoryConfig().addEntryConfig(ou);

        EntryConfig groups = new EntryConfig("cn=...,"+baseDn);
        groups.addObjectClass("groupOfUniqueNames");
        groups.addAttributeConfig(new EntryAttributeConfig("cn", EntryAttributeConfig.VARIABLE, "p.parentname", true));
        groups.addAttributeConfig(new EntryAttributeConfig("description", EntryAttributeConfig.VARIABLE, "p.description"));

        EntrySourceConfig groupsMapping = new EntrySourceConfig();
        groupsMapping.setAlias("p");
        groupsMapping.setSourceName("parents");
        groupsMapping.addFieldConfig(new EntryFieldConfig("parentname", EntryFieldConfig.VARIABLE, "cn"));
        groupsMapping.addFieldConfig(new EntryFieldConfig("description", EntryFieldConfig.VARIABLE, "description"));
        groups.addSourceConfig(groupsMapping);

        partitionConfig.getDirectoryConfig().addEntryConfig(groups);

        EntryConfig members = new EntryConfig("uid=child,cn=...,"+baseDn);
        members.addObjectClass("person");
        members.addObjectClass("organizationalPerson");
        members.addObjectClass("inetOrgPerson");
        members.addAttributesFromRdn();
        members.addAttributeConfig(new EntryAttributeConfig("description", EntryAttributeConfig.VARIABLE, "c.description"));

        EntrySourceConfig membersMapping = new EntrySourceConfig();
        membersMapping.setAlias("c");
        membersMapping.setSourceName("children");
        membersMapping.addFieldConfig(new EntryFieldConfig("parentname", EntryFieldConfig.VARIABLE, "p.parentname"));
        membersMapping.addFieldConfig(new EntryFieldConfig("description", EntryFieldConfig.VARIABLE, "description"));
        members.addSourceConfig(membersMapping);

        partitionConfig.getDirectoryConfig().addEntryConfig(members);

        PartitionManager partitionManager = penrose.getPartitionManager();
        partitionManager.createPartition(partitionConfig);
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
