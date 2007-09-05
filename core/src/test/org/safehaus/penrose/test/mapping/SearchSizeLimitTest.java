package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SearchSizeLimitTest extends StaticTestCase {

    public SearchSizeLimitTest() throws Exception {
    }

    public void testSizeLimitOne() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn("cn=group,"+baseDn);
        request.setFilter("(objectClass=*)");
        request.setSizeLimit(1);

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        assertTrue(response.hasNext());

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        assertEquals("cn=group,"+baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("cn");
        assertEquals("group", value);

        value = attributes.getValue("description");
        assertEquals("description", value);

        Collection values = attributes.getValues("uniqueMember");

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        try {
            response.hasNext();
            fail();
        } catch (LDAPException e) {
            assertEquals(LDAP.SIZE_LIMIT_EXCEEDED, e.getResultCode());
        }

        session.close();
    }

    public void testSizeLimitTwo() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn("cn=group,"+baseDn);
        request.setFilter("(objectClass=*)");
        request.setSizeLimit(2);

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        assertTrue(response.hasNext());

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        assertEquals("cn=group,"+baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("cn");
        assertEquals("group", value);

        value = attributes.getValue("description");
        assertEquals("description", value);

        Collection values = attributes.getValues("uniqueMember");

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        assertTrue(response.hasNext());

        searchResult = (SearchResult) response.next();
        dn = searchResult.getDn().toString();
        assertEquals("uid=member1,cn=group,"+baseDn, dn);

        attributes = searchResult.getAttributes();

        value = attributes.getValue("uid");
        assertEquals("member1", value);

        value = attributes.getValue("memberOf");
        assertEquals("group", value);

        try {
            response.hasNext();
            fail();
        } catch (LDAPException e) {
            assertEquals(LDAP.SIZE_LIMIT_EXCEEDED, e.getResultCode());
        }

        session.close();
    }

    public void testSizeLimitThree() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn("cn=group,"+baseDn);
        request.setFilter("(objectClass=*)");
        request.setSizeLimit(3);

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        assertTrue(response.hasNext());

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        assertEquals("cn=group,"+baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("cn");
        assertEquals("group", value);

        value = attributes.getValue("description");
        assertEquals("description", value);

        Collection values = attributes.getValues("uniqueMember");

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        assertTrue(response.hasNext());

        searchResult = (SearchResult) response.next();
        dn = searchResult.getDn().toString();
        assertEquals("uid=member1,cn=group,"+baseDn, dn);

        attributes = searchResult.getAttributes();

        value = attributes.getValue("uid");
        assertEquals("member1", value);

        value = attributes.getValue("memberOf");
        assertEquals("group", value);

        assertTrue(response.hasNext());

        searchResult = (SearchResult) response.next();
        dn = searchResult.getDn().toString();
        assertEquals("uid=member2,cn=group,"+baseDn, dn);

        attributes = searchResult.getAttributes();

        value = attributes.getValue("uid");
        assertEquals("member2", value);

        value = attributes.getValue("memberOf");
        assertEquals("group", value);

        assertFalse(response.hasNext());

        session.close();
    }
}
