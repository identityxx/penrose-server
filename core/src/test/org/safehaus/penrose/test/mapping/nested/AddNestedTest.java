package org.safehaus.penrose.test.mapping.nested;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.Attributes;

import junit.framework.Assert;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class AddNestedTest extends NestedTestCase {

    public AddNestedTest() throws Exception {
    }

    public void testAddingGroup() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new Attributes();
        attributes.setValue("cn", "group");
        attributes.setValue("description", "description");
        attributes.setValue("objectClass", "groupOfUniqueNames");

        session.add("cn=group,"+baseDn, attributes);

        session.close();

        Collection groups = executeQuery("select * from groups");
        Assert.assertEquals(groups.size(), 1);

        Map row = (Map)groups.iterator().next();
        Assert.assertEquals(row.get("GROUPNAME"), "group");
        Assert.assertEquals(row.get("DESCRIPTION"), "description");
    }

    public void testAddingMember() throws Exception {

        executeUpdate("insert into groups values ('group1', 'description')");
        executeUpdate("insert into groups values ('group2', 'description')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new Attributes();
        attributes.addValue("uid", "member");
        attributes.addValue("cn", "Member");
        attributes.addValue("objectClass", "person");
        attributes.addValue("objectClass", "organizationalPerson");
        attributes.addValue("objectClass", "inetOrgPerson");

        session.add("uid=member,cn=group2,"+baseDn, attributes);

        session.close();

        Collection groups = executeQuery("select * from members");
        Assert.assertEquals(groups.size(), 1);

        Map row = (Map)groups.iterator().next();
        Assert.assertEquals(row.get("USERNAME"), "member");
        Assert.assertEquals(row.get("GROUPNAME"), "group2");
        Assert.assertEquals(row.get("NAME"), "Member");
    }
}
