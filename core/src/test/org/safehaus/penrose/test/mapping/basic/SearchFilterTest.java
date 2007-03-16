package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SearchFilterTest extends BasicTestCase {

    public SearchFilterTest() throws Exception {
    }

    public void testSearchWithFilter() throws Exception {

        String groupnames[] = new String[] { "aabb", "bbcc", "ccdd" };
        String descriptions[] = new String[] { "AABB", "BBCC", "CCDD" };
        for (int i=0; i<groupnames.length; i++) {
            Collection params = new ArrayList();
            params.add(groupnames[i]);
            params.add(descriptions[i]);
            executeUpdate("insert into groups values (?, ?)", params);
        }

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                baseDn,
                "(cn=*b*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        assertTrue(response.hasNext());

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        assertEquals("cn=aabb,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        assertEquals("aabb", value);

        value = attributes.getOne("description");
        assertEquals("AABB", value);

        assertTrue(response.hasNext());

        entry = (Entry) response.next();
        dn = entry.getDn().toString();
        assertEquals("cn=bbcc,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("cn");
        assertEquals("bbcc", value);

        value = attributes.getOne("description");
        assertEquals("BBCC", value);

        assertFalse(response.hasNext());

        session.close();
    }

    public void testSearchWithNonExistentFilter() throws Exception {

        String groupnames[] = new String[] { "aabb", "bbcc", "ccdd" };
        String descriptions[] = new String[] { "AABB", "BBCC", "CCDD" };
        for (int i=0; i<groupnames.length; i++) {
            Collection params = new ArrayList();
            params.add(groupnames[i]);
            params.add(descriptions[i]);
            executeUpdate("insert into groups values (?, ?)", params);
        }

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                baseDn,
                "(cn=*f*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        assertFalse(response.hasNext());

        session.close();
    }
}
