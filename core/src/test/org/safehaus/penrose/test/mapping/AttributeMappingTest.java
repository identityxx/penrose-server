package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.directory.EntryAttributeConfig;

/**
 * @author Endi S. Dewata
 */
public class AttributeMappingTest extends TestCase {

    public void testClone() throws Exception {
        EntryAttributeConfig a1 = new EntryAttributeConfig();
        a1.setName("name");
        a1.setConstant("constant");
        a1.setRdn(true);

        EntryAttributeConfig a2 = (EntryAttributeConfig)a1.clone();
        assertEquals(a1.getName(), a2.getName());
        assertEquals(a1.getConstant(), a2.getConstant());
        assertEquals(a1.isRdn(), a2.isRdn());
    }

    public void testEquals() {
        EntryAttributeConfig a1 = new EntryAttributeConfig();
        a1.setName("name");
        a1.setConstant("constant");
        a1.setRdn(true);

        EntryAttributeConfig a2 = new EntryAttributeConfig();
        a2.setName("name");
        a2.setConstant("constant");
        a2.setRdn(true);

        assertEquals(a1, a2);
    }
}
