package org.safehaus.penrose.test.filter;

import junit.framework.TestCase;

import java.io.Reader;
import java.io.StringReader;

import org.safehaus.penrose.filter.*;

/**
 * @author Endi S. Dewata
 */
public class FilterTest extends TestCase {

    public void testEscape() throws Exception {
        String s1 = "(James\\Bond*)";
        String s2 = FilterTool.escape(s1);
        assertTrue("\\28James\\5cBond\\2a\\29".equals(s2));
    }

    public void testUnescape() throws Exception {
        String s1 = "\\28James\\5cBond\\2a\\29";
        String s2 = FilterTool.unescape(s1);
        assertTrue("(James\\Bond*)".equals(s2));
    }

    public void testSimple() throws Exception {
        Reader in = new StringReader("(filename=C:\\5cMyFile)");
        FilterParser parser = new FilterParser(in);

        Filter filter = parser.parse();
        assertTrue(filter instanceof SimpleFilter);

        SimpleFilter sf = (SimpleFilter)filter;

        String attribute = sf.getAttribute();
        assertTrue("filename".equals(attribute));

        String operator = sf.getOperator();
        assertTrue("=".equals(operator));

        Object value = sf.getValue();
        assertTrue("C:\\MyFile".equals(value));
    }

    public void testSubstring() throws Exception {
        Reader in = new StringReader("(cn=*\\2A*)");
        FilterParser parser = new FilterParser(in);

        Filter filter = parser.parse();
        assertTrue(filter instanceof SubstringFilter);

        SubstringFilter sf = (SubstringFilter)filter;

        String attribute = sf.getAttribute();
        assertTrue("cn".equals(attribute));

        Object values[] = sf.getSubstrings().toArray();
        assertTrue(values.length == 3);

        assertTrue(values[0].equals(SubstringFilter.STAR));
        assertTrue(values[1].equals("*"));
        assertTrue(values[2].equals(SubstringFilter.STAR));
    }
}