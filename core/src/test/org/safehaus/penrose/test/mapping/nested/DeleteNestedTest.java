package org.safehaus.penrose.test.mapping.nested;

import org.safehaus.penrose.session.PenroseSession;

import java.util.Collection;
import java.util.Map;

import junit.framework.Assert;

/**
 * @author Endi S. Dewata
 */
public class DeleteNestedTest extends NestedTestCase {

    public DeleteNestedTest() throws Exception {
    }

    public void testDeletingEntry() throws Exception {

        executeUpdate("insert into groups values ('group1', 'description1')");
        executeUpdate("insert into groups values ('group2', 'description2')");

        executeUpdate("insert into members values ('member', 'group1', 'Member1')");
        executeUpdate("insert into members values ('member', 'group2', 'Member2')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        session.delete("uid=member,cn=group1,"+baseDn);

        session.close();

        Collection results = executeQuery("select * from groups");
        Assert.assertEquals(2, results.size());

        results = executeQuery("select * from members");
        Assert.assertEquals(1, results.size());

        Map row = (Map)results.iterator().next();
        assertEquals("member", row.get("USERNAME"));
        assertEquals("group2", row.get("GROUPNAME"));
    }
}
