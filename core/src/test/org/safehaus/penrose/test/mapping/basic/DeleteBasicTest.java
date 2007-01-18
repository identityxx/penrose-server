package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.test.mapping.basic.BasicTestCase;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class DeleteBasicTest extends BasicTestCase {

    public DeleteBasicTest() throws Exception {
    }

    public void testDeletingEntry() throws Exception {

        executeUpdate("insert into groups values ('test', 'test')");

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        session.delete("cn=test,ou=Groups,dc=Example,dc=com");

        session.close();

        Collection results = executeQuery("select * from groups");
        assertEquals(results.size(), 0);
    }
}
