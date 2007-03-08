package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SearchBaseTest extends StaticTestCase {

    public SearchBaseTest() throws Exception {
    }

    public void testSearchingBaseOnGroup() throws Exception {

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("cn=group,"+baseDn, "(objectClass=*)", sc, results);

        boolean hasNext = results.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();
        attributes.print();

        Object value = attributes.getOne("cn");
        log.debug("cn: "+value);
        assertEquals("group", value);

        value = attributes.getOne("description");
        log.debug("description: "+value);
        assertEquals("description", value);

        Collection values = attributes.get("uniqueMember");

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            log.debug("uniqueMember: "+value);
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        hasNext = results.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        session.close();
    }

    public void testSearchingBaseOnMember() throws Exception {

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("uid=member1,cn=group,"+baseDn, "(objectClass=*)", sc, results);

        boolean hasNext = results.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("uid=member1,cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();
        attributes.print();

        Object value = attributes.getOne("uid");
        log.debug("uid: "+value);
        assertEquals("member1", value);

        value = attributes.getOne("memberOf");
        log.debug("memberOf: "+value);
        assertEquals("group", value);

        hasNext = results.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        session.close();
    }
}
