package org.safehaus.penrose.test.mapping.join;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.entry.Attributes;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class AddJoinTest extends JoinTestCase {

    public AddJoinTest() throws Exception {
    }

    public void testAddingEntry() throws Exception {

        Session session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        Attributes attributes = new Attributes();
        attributes.addValue("cn", "new");
        attributes.addValue("description", "description");
        attributes.addValue("uniqueMember", "member1");

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
