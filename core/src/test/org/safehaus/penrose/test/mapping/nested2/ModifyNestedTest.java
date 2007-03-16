package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.session.Session;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;

import junit.framework.Assert;

import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.DirContext;

/**
 * @author Endi S. Dewata
 */
public class ModifyNestedTest extends NestedTestCase {

    public ModifyNestedTest() throws Exception {
    }

    public void testDeletingMember() throws Exception {

        executeUpdate("insert into parents values ('group1', 'desc1')");
        executeUpdate("insert into parents values ('group2', 'desc2')");

        executeUpdate("insert into children values ('group1', 'child1')");
        executeUpdate("insert into children values ('group2', 'child2')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Collection modifications = new ArrayList();

        Attribute attribute = new BasicAttribute("description");
        attribute.add("newchild1");
        ModificationItem mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute);
        modifications.add(mi);

        session.modify("uid=child,cn=group1,"+baseDn, modifications);

        session.close();

        Collection groups = executeQuery("select * from children");
        Assert.assertEquals(groups.size(), 2);

        for (Iterator i=groups.iterator(); i.hasNext(); ) {
            Map row = (Map)i.next();

            if (row.get("PARENTNAME").equals("group1")) {
                Assert.assertEquals(row.get("DESCRIPTION"), "newchild1");

            } else if (row.get("PARENTNAME").equals("group2")) {
                Assert.assertEquals(row.get("DESCRIPTION"), "child2");
            } else {
                fail();
            }
        }
    }
}
