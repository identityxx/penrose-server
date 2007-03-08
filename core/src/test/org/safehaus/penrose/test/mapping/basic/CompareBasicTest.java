package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;

/**
 * @author Endi S. Dewata
 */
public class CompareBasicTest extends BasicTestCase {

    public CompareBasicTest() throws Exception {
    }

    public void testComparingEntry() throws Exception {

        executeUpdate("insert into groups values ('test', 'correct')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        boolean result = session.compare("cn=test,"+baseDn, "description", "correct");
        assertTrue(result);

        result = session.compare("cn=test,"+baseDn, "description", "wrong");
        assertFalse(result);

        session.close();
    }
}
