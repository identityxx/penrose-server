package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.Attributes;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SearchSubtreeTest extends StaticTestCase {

    public SearchSubtreeTest() throws Exception {
    }

    public void testSearchingOneLevelOnGroup() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = session.search("cn=group,"+baseDn, "(objectClass=*)");

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
