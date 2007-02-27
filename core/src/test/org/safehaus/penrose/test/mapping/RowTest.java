package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.RDNBuilder;

/**
 * @author Endi S. Dewata
 */
public class RowTest extends TestCase {

    public void testEquals() {
        RDNBuilder rb = new RDNBuilder();
        rb.set("name1", "value1");
        rb.set("name2", "value2");
        RDN r1 = rb.toRdn();

        rb.clear();
        rb.set("name1", "value1");
        rb.set("name2", "value2");
        RDN r2 = rb.toRdn();

        assertEquals(r1, r2);
    }
}
