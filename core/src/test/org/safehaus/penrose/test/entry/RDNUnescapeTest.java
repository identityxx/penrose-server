package org.safehaus.penrose.test.entry;

import junit.framework.TestCase;

import org.safehaus.penrose.ldap.LDAP;

/**
 * @author Endi S. Dewata
 */
public class RDNUnescapeTest extends TestCase {

    public void testUnescape() {
        String s1 = LDAP.unescape("James Bond");
        String s2 = "James Bond";
        assertEquals(s2, s1);
    }

    public void testUnescapeComma() {
        String s1 = LDAP.unescape("Bond\\, James");
        String s2 = "Bond, James";
        assertEquals(s2, s1);
    }

    public void testUnescapeEquals() {
        String s1 = LDAP.unescape("James Bond\\=007");
        String s2 = "James Bond=007";
        assertEquals(s2, s1);
    }

    public void testUnescapeCR() {
        String s1 = LDAP.unescape("\"James\nBond\"");
        String s2 = "James\nBond";
        assertEquals(s2, s1);
    }

    public void testUnescapeDoubleSpace() {
        String s1 = LDAP.unescape("\"James  Bond\"");
        String s2 = "James  Bond";
        assertEquals(s2, s1);
    }

    public void testUnescapeOutterSpace() {
        String s1 = LDAP.unescape("\" James Bond \"");
        String s2 = " James Bond ";
        assertEquals(s2, s1);
    }

    public void testUnscapePlus() {
        String s1 = LDAP.unescape("James\\+Bond");
        String s2 = "James+Bond";
        assertEquals(s2, s1);
    }

    public void testUnescapeLT() {
        String s1 = LDAP.unescape("James\\<Bond");
        String s2 = "James<Bond";
        assertEquals(s2, s1);
    }

    public void testUnescapeGT() {
        String s1 = LDAP.unescape("James\\>Bond");
        String s2 = "James>Bond";
        assertEquals(s2, s1);
    }

    public void testUnescapePount() {
        String s1 = LDAP.unescape("James\\#Bond");
        String s2 = "James#Bond";
        assertEquals(s2, s1);
    }

    public void testUnescapeSemiColon() {
        String s1 = LDAP.unescape("James\\;Bond");
        String s2 = "James;Bond";
        assertEquals(s2, s1);
    }

    public void testUnescapeBackspace() {
        String s1 = LDAP.unescape("James\\\\Bond");
        String s2 = "James\\Bond";
        assertEquals(s2, s1);
    }

    public void testUnescapeQuote() {
        String s1 = LDAP.unescape("James\\\"Bond");
        String s2 = "James\"Bond";
        assertEquals(s2, s1);
    }
}
