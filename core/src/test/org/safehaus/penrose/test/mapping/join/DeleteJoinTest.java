package org.safehaus.penrose.test.mapping.join;

import org.safehaus.penrose.session.PenroseSession;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class DeleteJoinTest extends JoinTestCase {

    public DeleteJoinTest() throws Exception {
    }

    public void testDeletingEntry() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into usergroups values ('group1', 'member1')");

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        session.delete("cn=group1,ou=Groups,dc=Example,dc=com");

        session.close();

        Collection results = executeQuery("select * from groups");
        assertEquals(results.size(), 0);

        results = executeQuery("select * from usergroups");
        assertEquals(results.size(), 0);
    }
}
