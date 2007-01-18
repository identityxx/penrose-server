package org.safehaus.penrose.test.mapping.join;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.test.mapping.join.JoinTestCase;

import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.DirContext;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class ModifyJoinTest extends JoinTestCase {

    public ModifyJoinTest() throws Exception {
    }

    public void testModifyingEntry() throws Exception {

        executeUpdate("insert into groups values ('group1', 'olddesc1')");
        executeUpdate("insert into usergroups values ('group1', 'member1')");

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        Collection modifications = new ArrayList();

        Attribute attribute = new BasicAttribute("description");
        attribute.add("newdesc1");
        ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute);

        modifications.add(mi);

        attribute = new BasicAttribute("uniqueMember");
        attribute.add("member2");
        mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute);

        modifications.add(mi);

        session.modify("cn=group1,ou=Groups,dc=Example,dc=com", modifications);

        session.close();

        Collection results = executeQuery("select * from groups");
        System.out.println("Groups: "+results);
        assertEquals(results.size(), 1);

        Map row = (Map)results.iterator().next();
        assertEquals(row.get("GROUPNAME"), "group1");
        assertEquals(row.get("DESCRIPTION"), "newdesc1");

        results = executeQuery("select * from usergroups");
        System.out.println("Usergroups: "+results);
        assertEquals(results.size(), 1);

        row = (Map)results.iterator().next();
        assertEquals(row.get("GROUPNAME"), "group1");
        assertEquals(row.get("USERNAME"), "member2");
    }
}
