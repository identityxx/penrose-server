package org.safehaus.penrose.test.mapping.basic;

import org.safehaus.penrose.session.PenroseSession;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
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

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new BasicAttributes();
        attributes.put("cn", "group");
        attributes.put("description", "description");
        attributes.put("objectClass", "groupOfUniqueNames");

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

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        Attributes attributes = new BasicAttributes();
        attributes.put("cn", "group");
        attributes.put("objectClass", "groupOfUniqueNames");

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
