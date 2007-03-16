package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.Session;

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
public class ModifyBasicTest extends BasicTestCase {

    public ModifyBasicTest() throws Exception {
    }

    public void testModifyingEntry() throws Exception {

        executeUpdate("insert into groups values ('group', 'olddesc')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attribute attribute = new BasicAttribute("description");
        attribute.add("newdesc");

        ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute);
        Collection modifications = new ArrayList();
        modifications.add(mi);

        session.modify("cn=group,"+baseDn, modifications);

        session.close();

        Collection results = executeQuery("select * from groups");
        assertEquals(results.size(), 1);

        Map row = (Map)results.iterator().next();
        assertEquals("group", row.get("GROUPNAME"));
        assertEquals("newdesc", row.get("DESCRIPTION"));
    }
}
