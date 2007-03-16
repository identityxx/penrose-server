package org.safehaus.penrose.test.mapping.nested;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.entry.AttributeValues;

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

        AttributeValues attributes = new AttributeValues();
        attributes.set("cn", "group");
        attributes.set("description", "description");
        attributes.set("objectClass", "groupOfUniqueNames");

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

        AttributeValues attributes = new AttributeValues();
        attributes.add("uid", "member");
        attributes.add("cn", "Member");
        attributes.add("objectClass", "person");
        attributes.add("objectClass", "organizationalPerson");
        attributes.add("objectClass", "inetOrgPerson");

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
