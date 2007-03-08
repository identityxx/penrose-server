package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.session.PenroseSession;

import java.util.Collection;
import java.util.Map;

import junit.framework.Assert;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;

/**
 * @author Endi S. Dewata
 */
public class AddNestedTest extends NestedTestCase {

    public AddNestedTest() throws Exception {
    }

    public void testAddingMember() throws Exception {

        executeUpdate("insert into parents values ('group1', 'desc1')");
        executeUpdate("insert into parents values ('group2', 'desc2')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new BasicAttributes();
        attributes.put("uid", "child");
        attributes.put("description", "child1");

        Attribute attribute = new BasicAttribute("objectClass");
        attribute.add("person");
        attribute.add("organizationalPerson");
        attribute.add("inetOrgPerson");
        attributes.put(attribute);

        session.add("uid=child,cn=group1,"+baseDn, attributes);

        session.close();

        Collection groups = executeQuery("select * from children");
        Assert.assertEquals(groups.size(), 1);

        Map row = (Map)groups.iterator().next();
        Assert.assertEquals(row.get("PARENTNAME"), "group1");
        Assert.assertEquals(row.get("DESCRIPTION"), "child1");
    }

}
