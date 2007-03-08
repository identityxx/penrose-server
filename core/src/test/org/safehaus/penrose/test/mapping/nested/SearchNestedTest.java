package org.safehaus.penrose.test.mapping.nested;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class SearchNestedTest extends NestedTestCase {

    Logger log = Logger.getLogger(getClass());

    public SearchNestedTest() throws Exception {
    }

    public void testSearchingEmptyDatabase() throws Exception {

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search(baseDn, "(objectClass=*)", sc, results);

        assertFalse(results.hasNext());

        session.close();
    }
}
