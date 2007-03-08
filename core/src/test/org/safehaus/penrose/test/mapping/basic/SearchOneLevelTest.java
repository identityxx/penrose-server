package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SearchOneLevelTest extends BasicTestCase {

    public SearchOneLevelTest() throws Exception {
    }

    public void testSearchEmptyDatabase() throws Exception {

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search(baseDn, "(objectClass=*)", sc, results);

        assertFalse(results.hasNext());

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

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search(baseDn, "(objectClass=*)", sc, results);

        //System.out.println("Results:");
        for (int i=0; i<groupnames.length; i++) {
            assertTrue(results.hasNext());

            Entry entry = (Entry)results.next();
            String dn = entry.getDn().toString();
            //System.out.println(" - "+dn);
            assertEquals("cn="+groupnames[i]+","+baseDn, dn);

            AttributeValues attributes = entry.getAttributeValues();

            Object value = attributes.getOne("cn");
            assertEquals(groupnames[i], value);

            value = attributes.getOne("description");
            assertEquals(descriptions[i], value);
        }

        session.close();
    }

}
