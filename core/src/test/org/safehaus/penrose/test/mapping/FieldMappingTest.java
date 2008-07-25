package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.directory.EntryFieldConfig;

/**
 * @author Endi S. Dewata
 */
public class FieldMappingTest extends TestCase {

    public void testClone() throws Exception {
        EntryFieldConfig f1 = new EntryFieldConfig();
        f1.setName("name");
        f1.setConstant("constant");

        EntryFieldConfig f2 = (EntryFieldConfig)f1.clone();
        assertEquals(f1.getName(), f2.getName());
        assertEquals(f1.getConstant(), f2.getConstant());
    }

    public void testEquals() {
        EntryFieldConfig f1 = new EntryFieldConfig();
        f1.setName("name");
        f1.setConstant("constant");

        EntryFieldConfig f2 = new EntryFieldConfig();
        f2.setName("name");
        f2.setConstant("constant");

        assertEquals(f1, f2);
    }
}
