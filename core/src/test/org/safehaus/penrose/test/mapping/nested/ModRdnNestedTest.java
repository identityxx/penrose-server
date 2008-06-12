package org.safehaus.penrose.test.mapping.nested;

import org.safehaus.penrose.session.Session;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModRdnNestedTest extends NestedTestCase {

    Logger log = Logger.getLogger(getClass());
    
    public ModRdnNestedTest() throws Exception {
    }

    public void testRenamingEntry() throws Exception {

        executeUpdate("insert into groups values ('group1', 'description1')");
        executeUpdate("insert into groups values ('group2', 'description2')");

        executeUpdate("insert into members values ('member', 'group1', 'Member1')");
        executeUpdate("insert into members values ('member', 'group2', 'Member2')");

        Session session = penrose.createSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        session.modrdn("uid=member,cn=group1,"+baseDn, "uid=newmember", true);

        session.close();

        Collection results = executeQuery("select * from groups");
        log.info("Groups: "+results);
        assertEquals(results.size(), 2);

        Iterator i = results.iterator();

        Map row = (Map)i.next();
        assertEquals(row.get("GROUPNAME"), "group1");
        assertEquals(row.get("DESCRIPTION"), "description1");

        row = (Map)i.next();
        assertEquals(row.get("GROUPNAME"), "group2");
        assertEquals(row.get("DESCRIPTION"), "description2");

        results = executeQuery("select * from members");
        log.info("Members: "+results);
        assertEquals(results.size(), 2);

        for (i = results.iterator(); i.hasNext(); ) {
            row = (Map)i.next();

            if (row.get("USERNAME").equals("newmember")) {
                assertEquals(row.get("GROUPNAME"), "group1");
                assertEquals(row.get("NAME"), "Member1");

            } else if (row.get("USERNAME").equals("member")) {
                assertEquals(row.get("GROUPNAME"), "group2");
                assertEquals(row.get("NAME"), "Member2");
            } else {
                fail();
            }
        }
    }
}
