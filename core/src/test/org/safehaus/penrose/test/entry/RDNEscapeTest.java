package org.safehaus.penrose.test.entry;

import junit.framework.TestCase;
import org.safehaus.penrose.ldap.LDAP;

/**
 * @author Endi S. Dewata
 */
public class RDNEscapeTest extends TestCase {

    public void testEscape() {
        String s1 = LDAP.escape("James Bond");
        String s2 = "James Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeComma() {
        String s1 = LDAP.escape("Bond, James");
        String s2 = "Bond\\, James";
        assertEquals(s2, s1);
    }

    public void testEscapeEquals() {
        String s1 = LDAP.escape("James Bond=007");
        String s2 = "James Bond\\=007";
        assertEquals(s2, s1);
    }

    public void testEscapeCR() {
        String s1 = LDAP.escape("James\nBond");
        String s2 = "\"James\nBond\"";
        assertEquals(s2, s1);
    }

    public void testEscapeDoubleSpace() {
        String s1 = LDAP.escape("James  Bond");
        String s2 = "\"James  Bond\"";
        assertEquals(s2, s1);
    }

    public void testEscapeOutterSpace() {
        String s1 = LDAP.escape(" James Bond ");
        String s2 = "\" James Bond \"";
        assertEquals(s2, s1);
    }

    public void testEscapePlus() {
        String s1 = LDAP.escape("James+Bond");
        String s2 = "James\\+Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeLT() {
        String s1 = LDAP.escape("James<Bond");
        String s2 = "James\\<Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeGT() {
        String s1 = LDAP.escape("James>Bond");
        String s2 = "James\\>Bond";
        assertEquals(s2, s1);
    }

    public void testEscapePount() {
        String s1 = LDAP.escape("James#Bond");
        String s2 = "James\\#Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeSemiColon() {
        String s1 = LDAP.escape("James;Bond");
        String s2 = "James\\;Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeBackspace() {
        String s1 = LDAP.escape("James\\Bond");
        String s2 = "James\\\\Bond";
        assertEquals(s2, s1);
    }

    public void testEscapeQuote() {
        String s1 = LDAP.escape("James\"Bond");
        String s2 = "James\\\"Bond";
        assertEquals(s2, s1);
    }
}
