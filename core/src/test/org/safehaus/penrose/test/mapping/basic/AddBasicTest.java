package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class AddBasicTest extends BasicTestCase {

    public AddBasicTest() throws Exception {
    }

    public void testAddingEntry() throws Exception {

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        Attributes attributes = new BasicAttributes();
        attributes.put("cn", "new");
        attributes.put("description", "description");
        session.add("cn=new,ou=Groups,dc=Example,dc=com", attributes);

        session.close();

        Collection results = executeQuery("select * from groups");
        assertEquals(results.size(), 1);

        Map row = (Map)results.iterator().next();
        assertEquals(row.get("GROUPNAME"), "new");
        assertEquals(row.get("DESCRIPTION"), "description");
    }
}
