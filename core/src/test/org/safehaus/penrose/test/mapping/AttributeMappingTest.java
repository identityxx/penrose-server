package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.directory.AttributeMapping;

/**
 * @author Endi S. Dewata
 */
public class AttributeMappingTest extends TestCase {

    public void testClone() throws Exception {
        AttributeMapping a1 = new AttributeMapping();
        a1.setName("name");
        a1.setConstant("constant");
        a1.setRdn(true);

        AttributeMapping a2 = (AttributeMapping)a1.clone();
        assertEquals(a1.getName(), a2.getName());
        assertEquals(a1.getConstant(), a2.getConstant());
        assertEquals(a1.isRdn(), a2.isRdn());
    }

    public void testEquals() {
        AttributeMapping a1 = new AttributeMapping();
        a1.setName("name");
        a1.setConstant("constant");
        a1.setRdn(true);

        AttributeMapping a2 = new AttributeMapping();
        a2.setName("name");
        a2.setConstant("constant");
        a2.setRdn(true);

        assertEquals(a1, a2);
    }
}
