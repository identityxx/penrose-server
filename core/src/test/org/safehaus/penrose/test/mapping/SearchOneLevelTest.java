package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;

/**
 * @author Endi S. Dewata
 */
public class SearchOneLevelTest extends StaticTestCase {

    public SearchOneLevelTest() throws Exception {
    }

    public void testSearchingOneLevelOnGroup() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                "cn=group,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        assertTrue(response.hasNext());

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        assertEquals(dn, "uid=member1,cn=group,"+baseDn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("uid");
        assertEquals("member1", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        assertTrue(response.hasNext());

        entry = (Entry) response.next();
        dn = entry.getDn().toString();
        assertEquals(dn, "uid=member2,cn=group,"+baseDn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("uid");
        assertEquals("member2", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        assertFalse(response.hasNext());

        session.close();
    }
}
