package org.safehaus.penrose.test.mapping.nested;

import org.safehaus.penrose.session.PenroseSession;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;

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

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new BasicAttributes();
        attributes.put("cn", "group");
        attributes.put("description", "description");
        attributes.put("objectClass", "groupOfUniqueNames");

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

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new BasicAttributes();
        attributes.put("uid", "member");
        attributes.put("cn", "Member");

        Attribute attribute = new BasicAttribute("objectClass");
        attribute.add("person");
        attribute.add("organizationalPerson");
        attribute.add("inetOrgPerson");
        attributes.put(attribute);

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
