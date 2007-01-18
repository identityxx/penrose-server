package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.test.mapping.basic.BasicTestCase;

/**
 * @author Endi S. Dewata
 */
public class CompareBasicTest extends BasicTestCase {

    public CompareBasicTest() throws Exception {
    }

    public void testComparingEntry() throws Exception {

        executeUpdate("insert into groups values ('test', 'correct')");

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        boolean result = session.compare("cn=test,ou=Groups,dc=Example,dc=com", "description", "correct");
        assertTrue(result);

        result = session.compare("cn=test,ou=Groups,dc=Example,dc=com", "description", "wrong");
        assertFalse(result);

        session.close();
    }
}
