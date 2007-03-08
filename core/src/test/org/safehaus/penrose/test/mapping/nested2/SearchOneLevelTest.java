package org.safehaus.penrose.test.mapping.nested2;

import org.apache.log4j.Logger;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;

/**
 * @author Endi S. Dewata
 */
public class SearchOneLevelTest extends NestedTestCase {

    Logger log = Logger.getLogger(getClass());

    public SearchOneLevelTest() throws Exception {
    }

    public void testSearchingOneLevelOnParent() throws Exception {

        executeUpdate("insert into parents values ('parent1', 'description1')");
        executeUpdate("insert into parents values ('parent2', 'description2')");
        executeUpdate("insert into parents values ('parent3', 'description3')");

        executeUpdate("insert into children values ('parent1', 'child1')");
        executeUpdate("insert into children values ('parent2', 'child2')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("cn=parent1,"+baseDn, "(objectClass=*)", sc, results);

        boolean hasNext = results.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry)results.next();
        String dn = entry.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals("uid=child,cn=parent1,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = (String)attributes.getOne("uid");
        log.debug("uid: "+ value);
        assertEquals("child", value);

        value = attributes.getOne("description");
        log.debug("description: "+value);
        assertEquals("child1", value);

        hasNext = results.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        int totalCount = results.getTotalCount();
        log.debug("totalCount: "+totalCount);
        assertEquals(1, totalCount);

        session.close();
    }

    public void testSearchingOneLevelOnParentWithNoChild() throws Exception {

        executeUpdate("insert into parents values ('parent1', 'description1')");
        executeUpdate("insert into parents values ('parent2', 'description2')");
        executeUpdate("insert into parents values ('parent3', 'description3')");

        executeUpdate("insert into children values ('parent1', 'child1')");
        executeUpdate("insert into children values ('parent2', 'child2')");

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);
        PenroseSearchResults results = new PenroseSearchResults();
        session.search("cn=parent3,"+baseDn, "(objectClass=*)", sc, results);

        boolean hasNext = results.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        int totalCount = results.getTotalCount();
        log.debug("totalCount: "+totalCount);
        assertEquals(0, totalCount);

        session.close();
    }
}
