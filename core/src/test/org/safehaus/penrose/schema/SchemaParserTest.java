package org.safehaus.penrose.schema;

import junit.framework.TestCase;

import java.io.StringReader;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SchemaParserTest extends TestCase {

    public void testObjectClass() throws Exception {
        String line = "objectclass ( 1.2.3.4.5.6.7 "+
                "NAME 'myObjectClass' "+
                "DESC 'This is a description' "+
                "SUP top )";

        StringReader sr = new StringReader(line);
        SchemaParser parser = new SchemaParser(sr);

        Collection list = parser.parse();

        assertFalse(list.isEmpty());

        System.out.println("Results:");
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            ObjectClass oc = (ObjectClass)i.next();
            System.out.println(" - "+oc.getName());
        }
    }

    public void testExtraParameters() throws Exception {
        String line = "objectclass ( 1.2.3.4.5.6.7 "+
                "NAME 'myObjectClass' "+
                "DESC 'This is a description' "+
                "SUP top "+
                "X-DS-USE 'internal' )";

        StringReader sr = new StringReader(line);
        SchemaParser parser = new SchemaParser(sr);

        Collection list = parser.parse();

        assertFalse(list.isEmpty());

        System.out.println("Results:");
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            ObjectClass oc = (ObjectClass)i.next();
            System.out.println(" - "+oc.getName());
        }
    }
}
