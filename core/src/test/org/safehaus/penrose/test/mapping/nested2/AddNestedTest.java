package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.entry.Attributes;

import java.util.Collection;
import java.util.Map;

import junit.framework.Assert;

/**
 * @author Endi S. Dewata
 */
public class AddNestedTest extends NestedTestCase {

    public AddNestedTest() throws Exception {
    }

    public void testAddingMember() throws Exception {

        executeUpdate("insert into parents values ('group1', 'desc1')");
        executeUpdate("insert into parents values ('group2', 'desc2')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new Attributes();
        attributes.addValue("uid", "child");
        attributes.addValue("description", "child1");
        attributes.addValue("objectClass", "person");
        attributes.addValue("objectClass", "organizationalPerson");
        attributes.addValue("objectClass", "inetOrgPerson");

        session.add("uid=child,cn=group1,"+baseDn, attributes);

        session.close();

        Collection groups = executeQuery("select * from children");
        Assert.assertEquals(groups.size(), 1);

        Map row = (Map)groups.iterator().next();
        Assert.assertEquals(row.get("PARENTNAME"), "group1");
        Assert.assertEquals(row.get("DESCRIPTION"), "child1");
    }

}
