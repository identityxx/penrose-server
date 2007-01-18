package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.test.mapping.basic.BasicTestCase;

import javax.naming.directory.*;
import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class ModifyBasicTest extends BasicTestCase {

    public ModifyBasicTest() throws Exception {
    }

    public void testModifyingEntry() throws Exception {

        executeUpdate("insert into groups values ('test', 'test')");

        PenroseSession session = penrose.newSession();
        session.setBindDn("uid=admin,ou=system");

        Attribute attribute = new BasicAttribute("description");
        attribute.add("description");

        ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute);
        Collection modifications = new ArrayList();
        modifications.add(mi);

        session.modify("cn=test,ou=Groups,dc=Example,dc=com", modifications);

        session.close();

        Collection results = executeQuery("select * from groups");
        assertEquals(results.size(), 1);

        Map row = (Map)results.iterator().next();
        assertEquals(row.get("GROUPNAME"), "test");
        assertEquals(row.get("DESCRIPTION"), "description");
    }
}
