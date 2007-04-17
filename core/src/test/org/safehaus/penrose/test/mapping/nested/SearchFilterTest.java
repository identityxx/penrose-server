package org.safehaus.penrose.test.mapping.nested;

import org.apache.log4j.Logger;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.Attributes;

/**
 * @author Endi S. Dewata
 */
public class SearchFilterTest extends NestedTestCase {

    Logger log = Logger.getLogger(getClass());

    public SearchFilterTest() throws Exception {
    }

    public void testSearchingGroupsWithFilter() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");
        executeUpdate("insert into groups values ('group3', 'desc3')");

        executeUpdate("insert into members values ('member1', 'group1', 'Member1')");
        executeUpdate("insert into members values ('member2', 'group1', 'Member2')");
        executeUpdate("insert into members values ('member3', 'group2', 'Member3')");
        executeUpdate("insert into members values ('member4', 'group2', 'Member4')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse<SearchResult> response = session.search(baseDn, "(description=desc2)");

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        SearchResult searchResult = (SearchResult) response.next();
        String dn = searchResult.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals("cn=group2,"+baseDn, dn);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        session.close();
    }

    public void testSearchingMembersWithFilter() throws Exception {

        executeUpdate("insert into groups values ('group1', 'desc1')");
        executeUpdate("insert into groups values ('group2', 'desc2')");
        executeUpdate("insert into groups values ('group3', 'desc3')");

        executeUpdate("insert into members values ('member1', 'group1', 'Member1')");
        executeUpdate("insert into members values ('member2', 'group1', 'Member2')");
        executeUpdate("insert into members values ('member3', 'group2', 'Member3')");
        executeUpdate("insert into members values ('member4', 'group2', 'Member4')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse<SearchResult> response = session.search(baseDn, "(memberOf=group2)");

        while (response.hasNext()) {
            SearchResult searchResult = (SearchResult) response.next();
            String dn = searchResult.getDn().toString();
            log.info("Checking "+dn+":");

            Attributes attributes = searchResult.getAttributes();
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

        log.debug("Total count: "+ response.getTotalCount());
        assertEquals(2, response.getTotalCount());

        session.close();
    }

}
