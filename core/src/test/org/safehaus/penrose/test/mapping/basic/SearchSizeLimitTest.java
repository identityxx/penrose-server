package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class SearchSizeLimitTest extends BasicTestCase {

    public SearchSizeLimitTest() throws Exception {
    }

    public void testSearchSizeLimitOne() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");

        Session session = penrose.createSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);
        request.setFilter("(objectClass=*)");
        request.setSizeLimit(1);

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        assertTrue(response.hasNext());

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        assertEquals(baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("ou");
        assertEquals("Groups", value);

        try {
            response.hasNext();
            fail();
        } catch (LDAPException e) {
            assertEquals(LDAP.SIZE_LIMIT_EXCEEDED, e.getResultCode());
        }

        session.close();
    }

    public void testSearchSizeLimitTwo() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");

        Session session = penrose.createSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);
        request.setFilter("(objectClass=*)");
        request.setSizeLimit(2);

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        assertTrue(response.hasNext());

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        assertEquals(baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("ou");
        assertEquals("Groups", value);

        assertTrue(response.hasNext());

        searchResult = (SearchResult) response.next();
        dn = searchResult.getDn().toString();
        assertEquals("cn=group1,"+baseDn, dn);

        attributes = searchResult.getAttributes();

        value = attributes.getValue("cn");
        assertEquals("group1", value);

        value = attributes.getValue("description");
        assertEquals("desc1", value);

        try {
            response.hasNext();
            fail();
        } catch (LDAPException e) {
            assertEquals(LDAP.SIZE_LIMIT_EXCEEDED, e.getResultCode());
        }

        session.close();
    }

    public void testSearchSizeLimitThree() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");

        Session session = penrose.createSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);
        request.setFilter("(objectClass=*)");
        request.setSizeLimit(3);

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        assertTrue(response.hasNext());

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals(baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("ou");
        assertEquals("Groups", value);

        assertTrue(response.hasNext());

        searchResult = (SearchResult) response.next();
        dn = searchResult.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals("cn=group1,"+baseDn, dn);

        attributes = searchResult.getAttributes();

        value = attributes.getValue("cn");
        assertEquals("group1", value);

        value = attributes.getValue("description");
        assertEquals("desc1", value);

        assertTrue(response.hasNext());

        searchResult = (SearchResult) response.next();
        dn = searchResult.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals("cn=group2,"+baseDn, dn);

        attributes = searchResult.getAttributes();

        value = attributes.getValue("cn");
        assertEquals("group2", value);

        value = attributes.getValue("description");
        assertEquals("desc2", value);

        assertFalse(response.hasNext());

        session.close();
    }
}
