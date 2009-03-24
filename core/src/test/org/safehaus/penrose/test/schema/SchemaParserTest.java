package org.safehaus.penrose.test.schema;

import junit.framework.TestCase;

import java.io.StringReader;
import java.util.Collection;

import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.SchemaParser;

/**
 * @author Endi S. Dewata
 */
public class SchemaParserTest extends TestCase {

    public void testParsingObjectClass() throws Exception {
        String line = "objectclass ( 1.2.3.4.5.6.7 "+
                "NAME 'myObjectClass' "+
                "DESC 'This is a description' "+
                "SUP top "+
                "X-DS-USE 'internal' )";

        StringReader sr = new StringReader(line);
        SchemaParser parser = new SchemaParser(sr);

        Schema schema = parser.parse();

        Collection<ObjectClass> list = schema.getObjectClasses();
        assertEquals(list.size(), 1);

        ObjectClass oc = list.iterator().next();
        assertEquals(oc.getOid(), "1.2.3.4.5.6.7");
        assertEquals(oc.getName(), "myObjectClass");
        assertEquals(oc.getDescription(), "This is a description");

        Collection superClasses = oc.getSuperClasses();
        assertEquals(superClasses.size(), 1);
        assertTrue(superClasses.contains("top"));
    }

    public void testParsingAttributeType() throws Exception {
        String line = "attributetype ( 1.2.3.4.5.6.7 "+
                "NAME 'myAttributeType' " +
                "DESC 'This is a description' " +
                "SUP top " +
                "EQUALITY distinguishedNameMatch " +
                "SYNTAX 1.2.3.4.5.6.7 " +
                "SINGLE-VALUE " +
                "NO-USER-MODIFICATION " +
                "USAGE dSAOperation )";

        StringReader sr = new StringReader(line);
        SchemaParser parser = new SchemaParser(sr);

        Schema schema = parser.parse();

        Collection<AttributeType> list = schema.getAttributeTypes();
        assertEquals(list.size(), 1);

        AttributeType at = list.iterator().next();
        assertEquals(at.getOid(), "1.2.3.4.5.6.7");
        assertEquals(at.getName(), "myAttributeType");
        assertEquals(at.getDescription(), "This is a description");
        assertEquals(at.getSuperClass(), "top");
        assertEquals(at.getEquality(), "distinguishedNameMatch");
        assertEquals(at.getSyntax(), "1.2.3.4.5.6.7");
        assertEquals(at.isSingleValued(), true);
        assertEquals(at.isModifiable(), false);
        assertEquals(at.getUsage(), "dSAOperation");
    }
}
