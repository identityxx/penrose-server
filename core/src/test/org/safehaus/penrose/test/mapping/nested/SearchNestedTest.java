package org.safehaus.penrose.test.mapping.nested;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class SearchNestedTest extends NestedTestCase {

    Logger log = Logger.getLogger(getClass());

    public SearchNestedTest() throws Exception {
    }

    public void testSearchingEmptyDatabase() throws Exception {

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse<SearchResult> response = session.search(
                baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_ONE
        );

        assertFalse(response.hasNext());

        session.close();
    }
}
