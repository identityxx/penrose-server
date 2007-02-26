package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.entry.RDN;

/**
 * @author Endi S. Dewata
 */
public class RowTest extends TestCase {

    public void testEquals() {
        RDN r1 = new RDN();
        r1.set("name1", "value1");
        r1.set("name2", "value2");

        RDN r2 = new RDN();
        r2.set("name1", "value1");
        r2.set("name2", "value2");

        assertEquals(r1, r2);
    }
}
