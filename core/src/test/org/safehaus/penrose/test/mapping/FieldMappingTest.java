package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.directory.FieldMapping;

/**
 * @author Endi S. Dewata
 */
public class FieldMappingTest extends TestCase {

    public void testClone() throws Exception {
        FieldMapping f1 = new FieldMapping();
        f1.setName("name");
        f1.setConstant("constant");

        FieldMapping f2 = (FieldMapping)f1.clone();
        assertEquals(f1.getName(), f2.getName());
        assertEquals(f1.getConstant(), f2.getConstant());
    }

    public void testEquals() {
        FieldMapping f1 = new FieldMapping();
        f1.setName("name");
        f1.setConstant("constant");

        FieldMapping f2 = new FieldMapping();
        f2.setName("name");
        f2.setConstant("constant");

        assertEquals(f1, f2);
    }
}
