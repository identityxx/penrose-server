package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.Penrose;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SearchAttributesTest extends StaticTestCase {

    public SearchAttributesTest() throws Exception {
    }

    public void testSearchDefaultAttributes() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                "cn=group,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE,
                response
        );

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        log.debug("cn: "+value);
        assertEquals("group", value);

        value = attributes.getOne("description");
        log.debug("description: "+value);
        assertEquals("description", value);

        Collection values = attributes.get("uniqueMember");
        log.debug("uniqueMember: "+values);

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        value = attributes.getOne("creatorsName");
        log.debug("creatorsName: "+value);
        assertNull(value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);
    }

    public void testSearchRegularAttributes() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn("cn=group,"+baseDn);
        request.setFilter("(objectClass=*)");
        request.setScope(SearchRequest.SCOPE_BASE);
        request.setAttributes(new String[] { "*" });

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        log.debug("cn: "+value);
        assertEquals("group", value);

        value = attributes.getOne("description");
        log.debug("description: "+value);
        assertEquals("description", value);

        Collection values = attributes.get("uniqueMember");
        log.debug("uniqueMember: "+values);

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        value = attributes.getOne("creatorsName");
        log.debug("creatorsName: "+value);
        assertNull(value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);
    }

    public void testSearchOperationalAttributes() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn("cn=group,"+baseDn);
        request.setFilter("(objectClass=*)");
        request.setScope(SearchRequest.SCOPE_BASE);
        request.setAttributes(new String[] { "+" });

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        log.debug("cn: "+value);
        assertNull(value);

        value = attributes.getOne("description");
        log.debug("description: "+value);
        assertNull(value);

        Collection values = attributes.get("uniqueMember");
        log.debug("uniqueMember: "+values);
        assertNull(value);

        value = attributes.getOne("creatorsName");
        log.debug("creatorsName: "+value);
        assertEquals(penroseConfig.getRootDn().toString(), value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);
    }

    public void testSearchSomeAttributes() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchRequest request = new SearchRequest();
        request.setDn("cn=group,"+baseDn);
        request.setFilter("(objectClass=*)");
        request.setScope(SearchRequest.SCOPE_BASE);
        request.setAttributes(new String[] { "cn", "uniqueMember", "creatorsName" });

        SearchResponse response = new SearchResponse();
        session.search(request, response);

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        log.debug("cn: "+value);
        assertEquals("group", value);

        value = attributes.getOne("description");
        log.debug("description: "+value);
        assertNull(value);

        Collection values = attributes.get("uniqueMember");
        log.debug("uniqueMember: "+values);

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        value = attributes.getOne("creatorsName");
        log.debug("creatorsName: "+value);
        assertEquals(penroseConfig.getRootDn().toString(), value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);
    }

    public void testSearchAllRootDSEAttributes() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                "",
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE,
                response
        );

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("", dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("vendorName");
        log.debug("vendorName: "+value);
        assertEquals(Penrose.VENDOR_NAME, value);

        value = attributes.getOne("vendorVersion");
        log.debug("vendorVersion: "+value);
        assertEquals(Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION, value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);
    }

}
