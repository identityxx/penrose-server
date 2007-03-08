package org.safehaus.penrose.test.mapping.nested;

import org.apache.log4j.Logger;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;

/**
 * @author Endi S. Dewata
 */
public class SearchBaseTest extends NestedTestCase {

    Logger log = Logger.getLogger(getClass());

    public SearchBaseTest() throws Exception {
    }

    public void testSearchingBaseOnGroup() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");
        executeUpdate("insert into groups values ('group3', 'desc3')");

        executeUpdate("insert into members values ('member1', 'group1', 'Member1')");
        executeUpdate("insert into members values ('member2', 'group1', 'Member2')");
        executeUpdate("insert into members values ('member3', 'group2', 'Member3')");
        executeUpdate("insert into members values ('member4', 'group2', 'Member4')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("cn=group2,"+baseDn, "(objectClass=*)", sc, results);

        assertTrue(results.hasNext());

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        assertEquals("cn=group2,"+baseDn, dn);

        assertFalse(results.hasNext());

        assertEquals(1, results.getTotalCount());

        session.close();
    }

    public void testSearchingBaseOnMember() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");
        executeUpdate("insert into groups values ('group3', 'desc3')");

        executeUpdate("insert into members values ('member1', 'group1', 'Member1')");
        executeUpdate("insert into members values ('member2', 'group1', 'Member2')");
        executeUpdate("insert into members values ('member3', 'group2', 'Member3')");
        executeUpdate("insert into members values ('member4', 'group2', 'Member4')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("uid=member3,cn=group2,"+baseDn, "(objectClass=*)", sc, results);

        assertTrue(results.hasNext());

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        assertEquals(dn, "uid=member3,cn=group2,"+baseDn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("memberOf");
        assertEquals("group2", value);

        assertFalse(results.hasNext());

        assertEquals(1, results.getTotalCount());

        session.close();
    }
}
