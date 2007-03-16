package org.safehaus.penrose.test.mapping;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SearchSubtreeTest extends StaticTestCase {

    public SearchSubtreeTest() throws Exception {
    }

    public void testSearchingOneLevelOnGroup() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();
        
        session.search("cn=group,"+baseDn, "(objectClass=*)", response);

        assertTrue(response.hasNext());

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        assertEquals("cn=group,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("cn");
        assertEquals("group", value);

        value = attributes.getOne("description");
        assertEquals("description", value);

        Collection values = attributes.get("uniqueMember");

        for (Iterator i = values.iterator(); i.hasNext(); ) {
            value = i.next();
            if (!value.equals("member1") && !value.equals("member2")) {
                fail();
            }
        }

        assertTrue(response.hasNext());

        entry = (Entry) response.next();
        dn = entry.getDn().toString();
        assertEquals("uid=member1,cn=group,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("uid");
        assertEquals("member1", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        assertTrue(response.hasNext());

        entry = (Entry) response.next();
        dn = entry.getDn().toString();
        assertEquals("uid=member2,cn=group,"+baseDn, dn);

        attributes = entry.getAttributeValues();

        value = attributes.getOne("uid");
        assertEquals("member2", value);

        value = attributes.getOne("memberOf");
        assertEquals("group", value);

        assertFalse(response.hasNext());

        session.close();
    }
}
