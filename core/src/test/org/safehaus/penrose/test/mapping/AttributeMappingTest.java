package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.mapping.AttributeMapping;

/**
 * @author Endi S. Dewata
 */
public class AttributeMappingTest extends TestCase {

    public void testClone() {
        AttributeMapping a1 = new AttributeMapping();
        a1.setName("name");
        a1.setType(AttributeMapping.CONSTANT);
        a1.setConstant("constant");
        a1.setRdn("true");
        a1.setLength(10);
        a1.setPrecision(2);
        a1.setOperational(true);

        AttributeMapping a2 = (AttributeMapping)a1.clone();
        assertEquals(a1.getName(), a2.getName());
        assertEquals(a1.getType(), a2.getType());
        assertEquals(a1.getConstant(), a2.getConstant());
        assertEquals(a1.getRdn(), a2.getRdn());
        assertEquals(a1.getLength(), a2.getLength());
        assertEquals(a1.getPrecision(), a2.getPrecision());
        assertEquals(a1.isOperational(), a2.isOperational());
    }

    public void testEquals() {
        AttributeMapping a1 = new AttributeMapping();
        a1.setName("name");
        a1.setType(AttributeMapping.CONSTANT);
        a1.setConstant("constant");
        a1.setRdn("true");
        a1.setLength(10);
        a1.setPrecision(2);
        a1.setOperational(true);

        AttributeMapping a2 = new AttributeMapping();
        a2.setName("name");
        a2.setType(AttributeMapping.CONSTANT);
        a2.setConstant("constant");
        a2.setRdn("true");
        a2.setLength(10);
        a2.setPrecision(2);
        a2.setOperational(true);

        assertEquals(a1, a2);
    }
}
