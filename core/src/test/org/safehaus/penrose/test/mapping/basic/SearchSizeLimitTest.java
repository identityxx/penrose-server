package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class SearchSizeLimitTest extends BasicTestCase {

    public SearchSizeLimitTest() throws Exception {
    }

    public void testSearchSizeLimitOne() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setSizeLimit(1);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search(baseDn, "(objectClass=*)", sc, results);

        assertTrue(results.hasNext());

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        assertEquals(baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("ou");
        assertEquals("Groups", value);

        try {
            results.hasNext();
            fail();
        } catch (LDAPException e) {
            assertEquals(LDAPException.SIZE_LIMIT_EXCEEDED, e.getResultCode());
        }

        session.close();
    }

    public void testSearchSizeLimitTwo() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setSizeLimit(2);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search(baseDn, "(objectClass=*)", sc, results);

        assertTrue(results.hasNext());

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        assertEquals(baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("ou");
        assertEquals("Groups", value);

        assertTrue(results.hasNext());

        entry = (Entry)results.next();
        dn = entry.getDn().toString();
        assertEquals("cn=group1,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("cn");
        assertEquals("group1", value);

        value = attributes.getOne("description");
        assertEquals("desc1", value);

        try {
            results.hasNext();
            fail();
        } catch (LDAPException e) {
            assertEquals(LDAPException.SIZE_LIMIT_EXCEEDED, e.getResultCode());
        }

        session.close();
    }

    public void testSearchSizeLimitThree() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setSizeLimit(3);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search(baseDn, "(objectClass=*)", sc, results);

        assertTrue(results.hasNext());

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals(baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("ou");
        assertEquals("Groups", value);

        assertTrue(results.hasNext());

        entry = (Entry)results.next();
        dn = entry.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals("cn=group1,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("cn");
        assertEquals("group1", value);

        value = attributes.getOne("description");
        assertEquals("desc1", value);

        assertTrue(results.hasNext());

        entry = (Entry)results.next();
        dn = entry.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals("cn=group2,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("cn");
        assertEquals("group2", value);

        value = attributes.getOne("description");
        assertEquals("desc2", value);

        assertFalse(results.hasNext());

        session.close();
    }
}
