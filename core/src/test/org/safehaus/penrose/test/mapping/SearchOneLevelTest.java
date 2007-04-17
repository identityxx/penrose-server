package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.Attributes;

/**
 * @author Endi S. Dewata
 */
public class SearchOneLevelTest extends StaticTestCase {

    public SearchOneLevelTest() throws Exception {
    }

    public void testSearchingOneLevelOnGroup() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse<SearchResult> response = session.search(
                "cn=group,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE
        );

        assertTrue(response.hasNext());

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        assertEquals(dn, "uid=member1,cn=group,"+baseDn);

        Attributes attributes = searchResult.getAttributes();

        Object value = attributes.getValue("uid");
        assertEquals("member1", value);

        value = attributes.getValue("memberOf");
        assertEquals("group", value);

        assertTrue(response.hasNext());

        searchResult = (SearchResult) response.next();
        dn = searchResult.getDn().toString();
        assertEquals(dn, "uid=member2,cn=group,"+baseDn);

        attributes = searchResult.getAttributes();

        value = attributes.getValue("uid");
        assertEquals("member2", value);

        value = attributes.getValue("memberOf");
        assertEquals("group", value);

        assertFalse(response.hasNext());

        session.close();
    }
}
