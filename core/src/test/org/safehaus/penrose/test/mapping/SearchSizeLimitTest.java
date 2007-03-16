package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
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

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        assertEquals("group", value);

        value = attributes.getOne("description");
        assertEquals("description", value);

        Collection values = attributes.get("uniqueMember");

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
            assertEquals(LDAPException.SIZE_LIMIT_EXCEEDED, e.getResultCode());
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

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        assertEquals("group", value);

        value = attributes.getOne("description");
        assertEquals("description", value);

        Collection values = attributes.get("uniqueMember");

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        assertTrue(response.hasNext());

        entry = (Entry) response.next();
        dn = entry.getDn().toString();
        assertEquals("uid=member1,cn=group,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("uid");
        assertEquals("member1", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        try {
            response.hasNext();
            fail();
        } catch (LDAPException e) {
            assertEquals(LDAPException.SIZE_LIMIT_EXCEEDED, e.getResultCode());
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

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        assertEquals("group", value);

        value = attributes.getOne("description");
        assertEquals("description", value);

        Collection values = attributes.get("uniqueMember");

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        assertTrue(response.hasNext());

        entry = (Entry) response.next();
        dn = entry.getDn().toString();
        assertEquals("uid=member1,cn=group,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("uid");
        assertEquals("member1", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        assertTrue(response.hasNext());

        entry = (Entry) response.next();
        dn = entry.getDn().toString();
        assertEquals("uid=member2,cn=group,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("uid");
        assertEquals("member2", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        assertFalse(response.hasNext());

        session.close();
    }
}
