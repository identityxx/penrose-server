package org.safehaus.penrose.test.mapping.nested;

import org.apache.log4j.Logger;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Attributes;

/**
 * @author Endi S. Dewata
 */
public class SearchOneLevelTest extends NestedTestCase {

    Logger log = Logger.getLogger(getClass());

    public SearchOneLevelTest() throws Exception {
    }

    public void testSearchingOneLevelOnRoot() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");
        executeUpdate("insert into groups values ('group3', 'desc3')");

        executeUpdate("insert into members values ('member1', 'group1', 'Member1')");
        executeUpdate("insert into members values ('member2', 'group1', 'Member2')");
        executeUpdate("insert into members values ('member3', 'group2', 'Member3')");
        executeUpdate("insert into members values ('member4', 'group2', 'Member4')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        while (response.hasNext()) {
            Entry entry = (Entry) response.next();
            String dn = entry.getDn().toString();
            log.info("Checking "+dn+":");

            Attributes attributes = entry.getAttributes();
            attributes.print();

            if (dn.equals("cn=group1,"+baseDn)) {
                Object value = attributes.getValue("description");
                assertEquals("desc1", value);

            } else if (dn.equals("cn=group2,"+baseDn)) {
                Object value = attributes.getValue("description");
                assertEquals("desc2", value);

            } else if (dn.equals("cn=group3,"+baseDn)) {
                Object value = attributes.getValue("description");
                assertEquals("desc3", value);

            } else {
                fail("Unexpected DN: "+dn);
            }
        }

        assertEquals(3, response.getTotalCount());

        session.close();
    }

    public void testSearchingOneLevelOnGroup() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");
        executeUpdate("insert into groups values ('group3', 'desc3')");

        executeUpdate("insert into members values ('member1', 'group1', 'Member1')");
        executeUpdate("insert into members values ('member2', 'group1', 'Member2')");
        executeUpdate("insert into members values ('member3', 'group2', 'Member3')");
        executeUpdate("insert into members values ('member4', 'group2', 'Member4')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                "cn=group2,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE,
                response
        );

        while (response.hasNext()) {
            Entry entry = (Entry) response.next();
            String dn = entry.getDn().toString();
            log.info("Checking "+dn+":");

            Attributes attributes = entry.getAttributes();
            attributes.print();

            if (dn.equals("uid=member3,cn=group2,"+baseDn)) {
                Object value = attributes.getValue("memberOf");
                assertEquals("group2", value);

            } else if (dn.equals("uid=member4,cn=group2,"+baseDn)) {
                Object value = attributes.getValue("memberOf");
                assertEquals("group2", value);

            } else {
                fail("Unexpected DN: "+dn);
            }
        }

        assertEquals(2, response.getTotalCount());

        session.close();
    }
}
