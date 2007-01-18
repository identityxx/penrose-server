package org.safehaus.penrose.test.mapping.join;

import org.safehaus.penrose.session.PenroseSession;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import java.util.Collection;
import java.util.Map;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class AddJoinTest extends JoinTestCase {

    public AddJoinTest() throws Exception {
    }

    public void testAddingEntry() throws Exception {

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        Attributes attributes = new BasicAttributes();
        attributes.put("cn", "new");
        attributes.put("description", "description");
        Attribute attribute = new BasicAttribute("uniqueMember");
        attribute.add("member1");
        attributes.put(attribute);
        session.add("cn=new,ou=Groups,dc=Example,dc=com", attributes);

        session.close();

        Collection groups = executeQuery("select * from groups");
        assertEquals(groups.size(), 1);

        Map row = (Map)groups.iterator().next();
        assertEquals(row.get("GROUPNAME"), "new");
        assertEquals(row.get("DESCRIPTION"), "description");

        Collection usergroups = executeQuery("select * from usergroups");
        assertEquals(usergroups.size(), 1);

        row = (Map)usergroups.iterator().next();
        assertEquals(row.get("GROUPNAME"), "new");
        assertEquals(row.get("USERNAME"), "member1");
    }
}
