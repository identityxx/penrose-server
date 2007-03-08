package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;

/**
 * @author Endi S. Dewata
 */
public class SearchOneLevelTest extends StaticTestCase {

    public SearchOneLevelTest() throws Exception {
    }

    public void testSearchingOneLevelOnGroup() throws Exception {

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("cn=group,"+baseDn, "(objectClass=*)", sc, results);

        assertTrue(results.hasNext());

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        assertEquals(dn, "uid=member1,cn=group,"+baseDn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("uid");
        assertEquals("member1", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        assertTrue(results.hasNext());

        entry = (Entry)results.next();
        dn = entry.getDn().toString();
        assertEquals(dn, "uid=member2,cn=group,"+baseDn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("uid");
        assertEquals("member2", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        assertFalse(results.hasNext());

        session.close();
    }
}
