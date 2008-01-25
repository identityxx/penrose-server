package org.safehaus.penrose.test.entry;

import junit.framework.TestCase;
import org.safehaus.penrose.ldap.LDAP;

/**
 * @author Endi S. Dewata
 */
public class RDNEscapeTest extends TestCase {

    public void testEscape() throws Exception {
        String s1 = LDAP.escape("James Bond");
        String s2 = "James Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeComma() throws Exception {
        String s1 = LDAP.escape("Bond, James");
        String s2 = "Bond\\, James";
        assertEquals(s2, s1);
    }

    public void testEscapeEquals() throws Exception {
        String s1 = LDAP.escape("James Bond=007");
        String s2 = "James Bond\\=007";
        assertEquals(s2, s1);
    }

    public void testEscapeCR() throws Exception {
        String s1 = LDAP.escape("James\nBond");
        String s2 = "\"James\nBond\"";
        assertEquals(s2, s1);
    }

    public void testEscapeDoubleSpace() throws Exception {
        String s1 = LDAP.escape("James  Bond");
        String s2 = "\"James  Bond\"";
        assertEquals(s2, s1);
    }

    public void testEscapeOutterSpace() throws Exception {
        String s1 = LDAP.escape(" James Bond ");
        String s2 = "\" James Bond \"";
        assertEquals(s2, s1);
    }

    public void testEscapePlus() throws Exception {
        String s1 = LDAP.escape("James+Bond");
        String s2 = "James\\+Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeLT() throws Exception {
        String s1 = LDAP.escape("James<Bond");
        String s2 = "James\\<Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeGT() throws Exception {
        String s1 = LDAP.escape("James>Bond");
        String s2 = "James\\>Bond";
        assertEquals(s2, s1);
    }

    public void testEscapePount() throws Exception {
        String s1 = LDAP.escape("James#Bond");
        String s2 = "James\\#Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeSemiColon() throws Exception {
        String s1 = LDAP.escape("James;Bond");
        String s2 = "James\\;Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeBackspace() throws Exception {
        String s1 = LDAP.escape("James\\Bond");
        String s2 = "James\\\\Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeQuote() throws Exception {
        String s1 = LDAP.escape("James\"Bond");
        String s2 = "James\\\"Bond";
        assertEquals(s2, s1);
    }
}
