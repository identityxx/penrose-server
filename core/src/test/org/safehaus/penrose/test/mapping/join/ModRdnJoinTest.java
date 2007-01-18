package org.safehaus.penrose.test.mapping.join;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.test.mapping.join.JoinTestCase;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class ModRdnJoinTest extends JoinTestCase {

    public ModRdnJoinTest() throws Exception {
    }

    public void testRenamingEntry() throws Exception {

        executeUpdate("insert into groups values ('old', 'description')");

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        session.modrdn("cn=old,ou=Groups,dc=Example,dc=com", "cn=new", true);

        session.close();

        Collection results = executeQuery("select * from groups");
        assertEquals(results.size(), 1);

        Map row = (Map)results.iterator().next();
        assertEquals(row.get("GROUPNAME"), "new");
        assertEquals(row.get("DESCRIPTION"), "description");
    }
}
