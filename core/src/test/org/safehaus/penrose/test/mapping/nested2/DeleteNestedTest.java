package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.session.Session;

import java.util.Collection;

import junit.framework.Assert;

/**
 * @author Endi S. Dewata
 */
public class DeleteNestedTest extends NestedTestCase {

    public DeleteNestedTest() throws Exception {
    }

    public void testDeletingMember() throws Exception {

        executeUpdate("insert into parents values ('group1', 'desc1')");
        executeUpdate("insert into parents values ('group2', 'desc2')");

        executeUpdate("insert into children values ('group1', 'child1')");

        Session session = penrose.createSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        session.delete("uid=child,cn=group1,"+baseDn);

        session.close();

        Collection groups = executeQuery("select * from children");
        Assert.assertEquals(groups.size(), 0);
    }
}
