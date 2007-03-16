package org.safehaus.penrose.test.mapping.join;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.session.SearchResult;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class SearchJoinTest extends JoinTestCase {

    public SearchJoinTest() throws Exception {
    }

    public void testSearchingEmptyDatabase() throws Exception {

        Session session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        SearchResponse response = new SearchResponse();

        session.search(
                "ou=Groups,dc=Example,dc=com",
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        assertFalse(response.hasNext());

        session.close();
    }

    public void testSearchingOneLevel() throws Exception {

        String groups[][] = new String[][] {
                new String[] { "group1", "desc1" },
                new String[] { "group2", "desc1" },
                new String[] { "group3", "desc1" }
        };

        for (int i=0; i<groups.length; i++) {
            Collection params = Arrays.asList(groups[i]);
            executeUpdate("insert into groups values (?, ?)", params);
        }

        String usergroups[][] = new String[][] {
                new String[] { "group1", "member1" },
                new String[] { "group1", "member2" },
                new String[] { "group2", "member1" },
                new String[] { "group3", "member3" }
        };

        for (int i=0; i<usergroups.length; i++) {
            Collection params = Arrays.asList(usergroups[i]);
            executeUpdate("insert into usergroups values (?, ?)", params);
        }

        Session session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        SearchResponse response = new SearchResponse();

        session.search(
                "ou=Groups,dc=Example,dc=com",
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        System.out.println("Results:");
        for (int i=0; i<groups.length; i++) {
            assertTrue(response.hasNext());

            SearchResult sr = (SearchResult) response.next();
            Entry entry = sr.getEntry();
            DN dn = entry.getDn();
            System.out.println(" - "+dn);
            assertTrue(dn.matches("cn="+groups[i][0]+",ou=Groups,dc=Example,dc=com"));

            Attributes attributes = entry.getAttributes();
            Attribute attribute = attributes.get("cn");
            assertEquals(attribute.getValue(), groups[i][0]);
            attribute = attributes.get("description");
            assertEquals(attribute.getValue(), groups[i][1]);
        }

        session.close();
    }

    public void testSearchingBase() throws Exception {

        String groupnames[] = new String[] { "abc", "def", "ghi" };
        String descriptions[] = new String[] { "ABC", "DEF", "GHI" };
        for (int i=0; i<groupnames.length; i++) {
            Collection params = new ArrayList();
            params.add(groupnames[i]);
            params.add(descriptions[i]);
            executeUpdate("insert into groups values (?, ?)", params);
        }

        Session session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        SearchResponse response = new SearchResponse();

        session.search(
                "cn=def,ou=Groups,dc=Example,dc=com",
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE,
                response
        );

        assertTrue(response.hasNext());

        SearchResult sr = (SearchResult) response.next();
        Entry entry = sr.getEntry();
        DN dn = entry.getDn();
        assertTrue(dn.matches("cn=def,ou=Groups,dc=Example,dc=com"));

        Attributes attributes = entry.getAttributes();
        Attribute attribute = attributes.get("cn");
        assertEquals(attribute.getValue(), "def");
        attribute = attributes.get("description");
        assertEquals(attribute.getValue(), "DEF");

        assertFalse(response.hasNext());

        session.close();
    }

    public void testSearchingNonExistentBase() throws Exception {

        String groupnames[] = new String[] { "abc", "def", "ghi" };
        String descriptions[] = new String[] { "ABC", "DEF", "GHI" };
        for (int i=0; i<groupnames.length; i++) {
            Collection params = new ArrayList();
            params.add(groupnames[i]);
            params.add(descriptions[i]);
            executeUpdate("insert into groups values (?, ?)", params);
        }

        Session session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        SearchResponse response = new SearchResponse();

        session.search(
                "cn=jkl,ou=Groups,dc=Example,dc=com",
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE,
                response
        );

        assertFalse(response.hasNext());

        session.close();
    }

    public void testSearchingOneLevelWithFilter() throws Exception {

        String groupnames[] = new String[] { "aabb", "bbcc", "ccdd" };
        String descriptions[] = new String[] { "AABB", "BBCC", "CCDD" };
        for (int i=0; i<groupnames.length; i++) {
            Collection params = new ArrayList();
            params.add(groupnames[i]);
            params.add(descriptions[i]);
            executeUpdate("insert into groups values (?, ?)", params);
        }

        Session session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        SearchResponse response = new SearchResponse();

        session.search(
                "ou=Groups,dc=Example,dc=com",
                "(cn=*b*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        assertTrue(response.hasNext());

        SearchResult sr = (SearchResult) response.next();
        Entry entry = sr.getEntry();
        DN dn = entry.getDn();
        assertTrue(dn.matches("cn=aabb,ou=Groups,dc=Example,dc=com"));

        Attributes attributes = entry.getAttributes();
        Attribute attribute = attributes.get("cn");
        assertEquals(attribute.getValue(), "aabb");
        attribute = attributes.get("description");
        assertEquals(attribute.getValue(), "AABB");

        assertTrue(response.hasNext());

        sr = (SearchResult) response.next();
        entry = sr.getEntry();
        dn = entry.getDn();
        assertTrue(dn.matches("cn=bbcc,ou=Groups,dc=Example,dc=com"));

        attributes = entry.getAttributes();
        attribute = attributes.get("cn");
        assertEquals(attribute.getValue(), "bbcc");
        attribute = attributes.get("description");
        assertEquals(attribute.getValue(), "BBCC");

        assertFalse(response.hasNext());

        session.close();
    }

    public void testSearchingOneLevelWithNonExistentFilter() throws Exception {

        String groupnames[] = new String[] { "aabb", "bbcc", "ccdd" };
        String descriptions[] = new String[] { "AABB", "BBCC", "CCDD" };
        for (int i=0; i<groupnames.length; i++) {
            Collection params = new ArrayList();
            params.add(groupnames[i]);
            params.add(descriptions[i]);
            executeUpdate("insert into groups values (?, ?)", params);
        }

        Session session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        SearchResponse response = new SearchResponse();

        session.search(
                "ou=Groups,dc=Example,dc=com",
                "(cn=*f*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        assertFalse(response.hasNext());

        session.close();
    }
}
