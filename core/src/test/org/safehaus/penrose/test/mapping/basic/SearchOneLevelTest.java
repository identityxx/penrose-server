package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Attributes;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SearchOneLevelTest extends BasicTestCase {

    public SearchOneLevelTest() throws Exception {
    }

    public void testSearchEmptyDatabase() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = session.search(
                baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE
        );

        assertFalse(response.hasNext());

        session.close();
    }

    public void testSearchOneLevel() throws Exception {

        String groupnames[] = new String[] { "abc", "def", "ghi" };
        String descriptions[] = new String[] { "ABC", "DEF", "GHI" };
        for (int i=0; i<groupnames.length; i++) {
            Collection params = new ArrayList();
            params.add(groupnames[i]);
            params.add(descriptions[i]);
            executeUpdate("insert into groups values (?, ?)", params);
        }

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = session.search(
                baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE
        );

        //System.out.println("Results:");
        for (int i=0; i<groupnames.length; i++) {
            assertTrue(response.hasNext());

            Entry entry = (Entry) response.next();
            String dn = entry.getDn().toString();
            //System.out.println(" - "+dn);
            assertEquals("cn="+groupnames[i]+","+baseDn, dn);

            Attributes attributes = entry.getAttributes();

            Object value = attributes.getValue("cn");
            assertEquals(groupnames[i], value);

            value = attributes.getValue("description");
            assertEquals(descriptions[i], value);
        }

        session.close();
    }

}
