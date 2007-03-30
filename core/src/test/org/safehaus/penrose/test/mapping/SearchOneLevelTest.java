package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.Attributes;

/**
 * @author Endi S. Dewata
 */
public class SearchOneLevelTest extends StaticTestCase {

    public SearchOneLevelTest() throws Exception {
    }

    public void testSearchingOneLevelOnGroup() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = session.search(
                "cn=group,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE
        );

        assertTrue(response.hasNext());

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        assertEquals(dn, "uid=member1,cn=group,"+baseDn);

        Attributes attributes = entry.getAttributes();

        Object value = attributes.getValue("uid");
        assertEquals("member1", value);

        value = attributes.getValue("memberOf");
        assertEquals("group", value);

        assertTrue(response.hasNext());

        entry = (Entry) response.next();
        dn = entry.getDn().toString();
        assertEquals(dn, "uid=member2,cn=group,"+baseDn);

        attributes = entry.getAttributes();

        value = attributes.getValue("uid");
        assertEquals("member2", value);

        value = attributes.getValue("memberOf");
        assertEquals("group", value);

        assertFalse(response.hasNext());

        session.close();
    }
}
