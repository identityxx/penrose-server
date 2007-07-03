package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.Session;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class DeleteBasicTest extends BasicTestCase {

    public DeleteBasicTest() throws Exception {
    }

    public void testDeletingEntry() throws Exception {

        executeUpdate("insert into groups values ('test', 'test')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        session.delete("cn=test,"+baseDn);

        session.close();

        Collection results = executeQuery("select * from groups");
        assertEquals(results.size(), 0);
    }
}
