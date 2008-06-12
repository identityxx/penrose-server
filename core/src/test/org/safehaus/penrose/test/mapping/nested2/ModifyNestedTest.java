package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.Modification;
import org.safehaus.penrose.ldap.Attribute;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;

import junit.framework.Assert;

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

        Session session = penrose.createSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Collection<Modification> modifications = new ArrayList<Modification>();

        Attribute attribute = new Attribute("description");
        attribute.addValue("newchild1");

        Modification mi = new Modification(Modification.REPLACE, attribute);
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
