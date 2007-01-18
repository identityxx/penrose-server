package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.mapping.Row;

/**
 * @author Endi S. Dewata
 */
public class RowTest extends TestCase {

    public void testEquals() {
        Row r1 = new Row();
        r1.set("name1", "value1");
        r1.set("name2", "value2");

        Row r2 = new Row();
        r2.set("name1", "value1");
        r2.set("name2", "value2");

        assertEquals(r1, r2);
    }
}
