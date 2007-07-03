package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.mapping.Expression;

/**
 * @author Endi S. Dewata
 */
public class ExpressionTest extends TestCase {

    public void testClone() {
        Expression e1 = new Expression();
        e1.setForeach("foreach");
        e1.setScript("script");
        e1.setVar("var");

        Expression e2 = (Expression)e1.clone();
        assertEquals(e1.getForeach(), e2.getForeach());
        assertEquals(e1.getScript(), e2.getScript());
        assertEquals(e1.getVar(), e2.getVar());
    }

    public void testEquals() {
        Expression e1 = new Expression();
        e1.setForeach("foreach");
        e1.setScript("script");
        e1.setVar("var");

        Expression e2 = new Expression();
        e2.setForeach("foreach");
        e2.setScript("script");
        e2.setVar("var");

        assertEquals(e1, e2);
    }
}
