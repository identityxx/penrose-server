package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.Attributes;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SearchBaseTest extends StaticTestCase {

    public SearchBaseTest() throws Exception {
    }

    public void testSearchingBaseOnGroup() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = session.search(
                "cn=group,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE
        );

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("cn=group,"+baseDn, dn);

        Attributes attributes = entry.getAttributes();
        attributes.print();

        Object value = attributes.getValue("cn");
        log.debug("cn: "+value);
        assertEquals("group", value);

        value = attributes.getValue("description");
        log.debug("description: "+value);
        assertEquals("description", value);

        Collection values = attributes.getValues("uniqueMember");

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            log.debug("uniqueMember: "+value);
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        session.close();
    }

    public void testSearchingBaseOnMember() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = session.search(
                "uid=member1,cn=group,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE
        );

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        log.debug("dn: "+dn);
        assertEquals("uid=member1,cn=group,"+baseDn, dn);

        Attributes attributes = entry.getAttributes();
        attributes.print();

        Object value = attributes.getValue("uid");
        log.debug("uid: "+value);
        assertEquals("member1", value);

        value = attributes.getValue("memberOf");
        log.debug("memberOf: "+value);
        assertEquals("group", value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        session.close();
    }
}
