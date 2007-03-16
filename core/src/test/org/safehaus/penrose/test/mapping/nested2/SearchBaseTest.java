package org.safehaus.penrose.test.mapping.nested2;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class SearchBaseTest extends NestedTestCase {

    Logger log = Logger.getLogger(getClass());

    public SearchBaseTest() throws Exception {
    }

    public void testSearchingBaseOnParent() throws Exception {

        executeUpdate("insert into parents values ('parent1', 'description1')");
        executeUpdate("insert into parents values ('parent2', 'description2')");
        executeUpdate("insert into parents values ('parent3', 'description3')");

        executeUpdate("insert into children values ('parent1', 'child1')");
        executeUpdate("insert into children values ('parent2', 'child2')");
        executeUpdate("insert into children values ('parent3', 'child3')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                "cn=parent1,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE,
                response
        );

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry sr = (Entry) response.next();
        String dn = sr.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals("cn=parent1,"+baseDn, dn);

        AttributeValues attributes = sr.getAttributeValues();

        Object value = attributes.getOne("description");
        log.debug("description: "+ value);
        assertEquals("description1", value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        int totalCount = response.getTotalCount();
        log.debug("totalCount: "+totalCount);
        assertEquals(1, totalCount);

        session.close();
    }

    public void testSearchingBaseOnChild() throws Exception {

        executeUpdate("insert into parents values ('parent1', 'description1')");
        executeUpdate("insert into parents values ('parent2', 'description2')");
        executeUpdate("insert into parents values ('parent3', 'description3')");

        executeUpdate("insert into children values ('parent1', 'child1')");
        executeUpdate("insert into children values ('parent2', 'child2')");
        executeUpdate("insert into children values ('parent3', 'child3')");

        Session session = penrose.newSession();
        session.bind(penroseConfig.getRootDn(), penroseConfig.getRootPassword());

        SearchResponse response = new SearchResponse();

        session.search(
                "uid=child,cn=parent1,"+baseDn,
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE,
                response
        );

        boolean hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertTrue(hasNext);

        Entry entry = (Entry) response.next();
        String dn = entry.getDn().toString();
        log.debug("DN: "+dn);
        assertEquals("uid=child,cn=parent1,"+baseDn, dn);

        AttributeValues attributes = entry.getAttributeValues();

        Object value = attributes.getOne("uid");
        log.debug("uid: "+ value);
        assertEquals("child", value);

        value = attributes.getOne("description");
        log.debug("description: "+value);
        assertEquals("child1", value);

        hasNext = response.hasNext();
        log.debug("hasNext: "+hasNext);
        assertFalse(hasNext);

        int totalCount = response.getTotalCount();
        log.debug("totalCount: "+totalCount);
        assertEquals(1, totalCount);

        session.close();
    }
}
