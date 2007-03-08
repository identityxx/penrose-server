package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class ModRdnBasicTest extends BasicTestCase {

    public ModRdnBasicTest() throws Exception {
    }

    public void testRenamingEntry() throws Exception {

        executeUpdate("insert into groups values ('old', 'description')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        session.modrdn("cn=old,"+baseDn, "cn=new", true);

        session.close();

        Collection results = executeQuery("select * from groups");
        assertEquals(results.size(), 1);

        Map row = (Map)results.iterator().next();
        assertEquals(row.get("GROUPNAME"), "new");
        assertEquals(row.get("DESCRIPTION"), "description");
    }
}
