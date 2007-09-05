package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.Attributes;
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

        SearchResponse response = session.search(
                "cn=group,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE
        );

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("cn");
        log.debug("cn: "+value);
        assertEquals("group", value);

        value = attributes.getValue("description");
        log.debug("description: "+value);
        assertEquals("description", value);

        Collection values = attributes.getValues("uniqueMember");
        log.debug("uniqueMember: "+values);

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        value = attributes.getValue("creatorsName");
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

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("cn");
        log.debug("cn: "+value);
        assertEquals("group", value);

        value = attributes.getValue("description");
        log.debug("description: "+value);
        assertEquals("description", value);

        Collection values = attributes.getValues("uniqueMember");
        log.debug("uniqueMember: "+values);

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        value = attributes.getValue("creatorsName");
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

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("cn");
        log.debug("cn: "+value);
        assertNull(value);

        value = attributes.getValue("description");
        log.debug("description: "+value);
        assertNull(value);

        Collection values = attributes.getValues("uniqueMember");
        log.debug("uniqueMember: "+values);
        assertNull(value);

        value = attributes.getValue("creatorsName");
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

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("cn");
        log.debug("cn: "+value);
        assertEquals("group", value);

        value = attributes.getValue("description");
        log.debug("description: "+value);
        assertNull(value);

        Collection values = attributes.getValues("uniqueMember");
        log.debug("uniqueMember: "+values);

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        value = attributes.getValue("creatorsName");
        log.debug("creatorsName: "+value);
        assertEquals(penroseConfig.getRootDn().toString(), value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);
    }

    public void testSearchAllRootDSEAttributes() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = session.search(
                "",
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE
        );

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("", dn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("vendorName");
        log.debug("vendorName: "+value);
        assertEquals(Penrose.VENDOR_NAME, value);

        value = attributes.getValue("vendorVersion");
        log.debug("vendorVersion: "+value);
        assertEquals(Penrose.PRODUCT_NAME+" Server "+Penrose.PRODUCT_VERSION, value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);
    }

}
