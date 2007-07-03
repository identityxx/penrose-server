package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.Attributes;

import java.util.Collection;
import java.util.Map;

import junit.framework.Assert;

/**
 * @author Endi S. Dewata
 */
public class AddBasicTest extends BasicTestCase {

    public AddBasicTest() throws Exception {
    }

    public void testAddingEntry() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new Attributes();
        attributes.setValue("cn", "group");
        attributes.setValue("description", "description");
        attributes.setValue("objectClass", "groupOfUniqueNames");

        session.add("cn=group,"+baseDn, attributes);

        session.close();

        Collection results = executeQuery("select * from groups");
        log.debug("Groups: "+results);
        Assert.assertEquals(results.size(), 1);

        Map row = (Map)results.iterator().next();
        log.debug("RDN: "+row);
        Assert.assertEquals(row.get("GROUPNAME"), "group");
        Assert.assertEquals(row.get("DESCRIPTION"), "description");
    }

    public void testAddingPartialEntry() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new Attributes();
        attributes.setValue("cn", "group");
        attributes.setValue("objectClass", "groupOfUniqueNames");

        session.add("cn=group,"+baseDn, attributes);

        session.close();

        Collection results = executeQuery("select * from groups");
        log.debug("Groups: "+results);
        Assert.assertEquals(results.size(), 1);

        Map row = (Map)results.iterator().next();
        log.debug("RDN: "+row);
        Assert.assertEquals(row.get("GROUPNAME"), "group");
        Assert.assertEquals(row.get("DESCRIPTION"), null);
    }
}
