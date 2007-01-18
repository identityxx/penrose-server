package org.safehaus.penrose.test.quick.jdbc;

import junit.framework.TestCase;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.DefaultEngine;
import org.safehaus.penrose.jdbc.JDBCAdapter;
import org.safehaus.penrose.connector.AdapterConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.apache.log4j.*;

import javax.naming.directory.SearchResult;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SimpleJDBCTest extends JDBCTestCase {

    public SimpleJDBCTest() throws Exception {
    }

    public void setUp() throws Exception {
        executeUpdate("create table users ("+
                "username varchar(10), "+
                "primary key (username))"
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
        partition.addConnectionConfig(connectionConfig);

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setName("users");
        sourceConfig.setConnectionName("HSQLDB");
        sourceConfig.setParameter("table", "users");
        sourceConfig.addFieldConfig(new FieldConfig("username", "true"));
        partition.addSourceConfig(sourceConfig);

        EntryMapping ou = new EntryMapping();
        ou.setDn("ou=Users,dc=Example,dc=com");
        ou.addObjectClass("organizationalUnit");
        ou.addAttributeMapping(new AttributeMapping("ou", AttributeMapping.CONSTANT, "Users", true));
        partition.addEntryMapping(ou);

        EntryMapping users = new EntryMapping();
        users.setDn("uid=...,ou=Users,dc=Example,dc=com");
        users.addObjectClass("person");
        users.addAttributeMapping(new AttributeMapping("uid", AttributeMapping.VARIABLE, "u.username", true));

        SourceMapping sourceMapping = new SourceMapping();
        sourceMapping.setName("u");
        sourceMapping.setSourceName("users");
        sourceMapping.addFieldMapping(new FieldMapping("username", FieldMapping.VARIABLE, "uid"));
        users.addSourceMapping(sourceMapping);

        partition.addEntryMapping(users);

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);

        PartitionManager partitionManager = penrose.getPartitionManager();
        partitionManager.addPartition(partition);

        penrose.start();
    }


    public void tearDown() throws Exception {
        penrose.stop();

        executeUpdate("drop table users");
    }

    public void testSearchingEmptyDatabase() throws Exception {

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("ou=Users,dc=Example,dc=com", "(objectClass=*)", sc, results);

        assertFalse(results.hasNext());

        session.close();
    }

    public void testSearchingOneLevel() throws Exception {

        String usernames[] = new String[] { "abc", "def", "ghi" };
        for (int i=0; i<usernames.length; i++) {
            Collection params = new ArrayList();
            params.add(usernames[i]);
            executeUpdate("insert into users values (?)", params);
        }

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("ou=Users,dc=Example,dc=com", "(objectClass=*)", sc, results);

        System.out.println("Results:");
        for (int i=0; i<usernames.length; i++) {
            assertTrue(results.hasNext());

            SearchResult sr = (SearchResult)results.next();
            String dn = sr.getName();
            System.out.println(" - "+dn);
            assertEquals(dn, "uid="+usernames[i]+",ou=Users,dc=Example,dc=com");
        }

        session.close();
    }

    public void testSearchingBase() throws Exception {

        String usernames[] = new String[] { "abc", "def", "ghi" };
        for (int i=0; i<usernames.length; i++) {
            Collection params = new ArrayList();
            params.add(usernames[i]);
            executeUpdate("insert into users values (?)", params);
        }

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("uid=def,ou=Users,dc=Example,dc=com", "(objectClass=*)", sc, results);

        assertTrue(results.hasNext());

        SearchResult sr = (SearchResult)results.next();
        String dn = sr.getName();
        assertEquals(dn, "uid=def,ou=Users,dc=Example,dc=com");

        assertFalse(results.hasNext());

        session.close();
    }

    public void testSearchingNonExistentBase() throws Exception {

        String usernames[] = new String[] { "abc", "def", "ghi" };
        for (int i=0; i<usernames.length; i++) {
            Collection params = new ArrayList();
            params.add(usernames[i]);
            executeUpdate("insert into users values (?)", params);
        }

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("uid=jkl,ou=Users,dc=Example,dc=com", "(objectClass=*)", sc, results);

        assertFalse(results.hasNext());

        session.close();
    }

    public void testSearchingOneLevelWithFilter() throws Exception {

        String usernames[] = new String[] { "aabb", "bbcc", "ccdd" };
        for (int i=0; i<usernames.length; i++) {
            Collection params = new ArrayList();
            params.add(usernames[i]);
            executeUpdate("insert into users values (?)", params);
        }

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("ou=Users,dc=Example,dc=com", "(uid=*b*)", sc, results);

        assertTrue(results.hasNext());

        SearchResult sr = (SearchResult)results.next();
        String dn = sr.getName();
        assertEquals(dn, "uid=aabb,ou=Users,dc=Example,dc=com");

        assertTrue(results.hasNext());

        sr = (SearchResult)results.next();
        dn = sr.getName();
        assertEquals(dn, "uid=bbcc,ou=Users,dc=Example,dc=com");

        assertFalse(results.hasNext());

        session.close();
    }

    public void testSearchingOneLevelWithNonExistentFilter() throws Exception {

        String usernames[] = new String[] { "aabb", "bbcc", "ccdd" };
        for (int i=0; i<usernames.length; i++) {
            Collection params = new ArrayList();
            params.add(usernames[i]);
            executeUpdate("insert into users values (?)", params);
        }

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("ou=Users,dc=Example,dc=com", "(uid=*f*)", sc, results);

        assertFalse(results.hasNext());

        session.close();
    }
}
