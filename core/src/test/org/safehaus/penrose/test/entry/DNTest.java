package org.safehaus.penrose.test.entry;

import junit.framework.TestCase;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.RDN;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class DNTest extends TestCase {

    Logger log = Logger.getLogger(getClass());

    public void testGetRdn() throws Exception {
        DN dn = new DN("cn=James Bond,ou=Users,dc=Example,dc=com");
        RDN rdn = dn.getRdn();

        assertEquals(rdn.getNames().size(), 1);

        String cn = rdn.getName();
        assertEquals(cn, "cn");

        String value = (String)rdn.get(cn);
        assertEquals(value, "James Bond");
    }

    public void testGetParentDn() throws Exception {
        DN dn = new DN("cn=James Bond,ou=Users,dc=Example,dc=com");

        DN parentDn1 = new DN("ou=Users,dc=Example,dc=com");
        DN parentDn2 = dn.getParentDn();

        assertEquals(parentDn1, parentDn2);
    }

    public void testEndsWith() throws Exception {
        DN dn = new DN("ou=Users,dc=Example,dc=com");

        assertTrue(dn.endsWith("ou=Users,dc=Example,dc=com"));
        assertTrue(dn.endsWith("DC=example, DC=com"));
        assertTrue(dn.endsWith(""));
        assertFalse(dn.endsWith("cn=James Bond,ou=Users,dc=Example,dc=com"));
        assertFalse(dn.endsWith("ou=Groups,dc=Example,dc=com"));
    }

    public void testMatches() throws Exception {
        DN dn1 = new DN("cn=James Bond,ou=Users,dc=Example,dc=com");
        DN dn2 = new DN("cn=..., ou=Users, DC=example, DC=com");
        DN suffix = new DN("dc=Example,dc=com");

        assertTrue(dn1.matches(dn2));
        assertFalse(dn1.matches(suffix));
    }

    public void testParsingSimpleDn() throws Exception {
        DN dn = new DN("cn=James Bond,ou=Users,dc=Example,dc=com");

        assertEquals(dn.getLength(), 4);

        RDN rdn = dn.getRdn(0);
        Collection names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("cn"));
        assertEquals(rdn.get("cn"), "James Bond");

        rdn = dn.getRdn(1);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("ou"));
        assertEquals(rdn.get("ou"), "Users");

        rdn = dn.getRdn(2);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "Example");

        rdn = dn.getRdn(3);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "com");

    }

    public void testParsingDnWithEscapedCharacters() throws Exception {
        DN dn = new DN("cn=Bond\\, James,ou=Agents\\, Secret,dc=Example,dc=com");

        assertEquals(dn.getLength(), 4);

        RDN rdn = dn.getRdn(0);
        Collection names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("cn"));
        assertEquals(rdn.get("cn"), "Bond, James");

        rdn = dn.getRdn(1);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("ou"));
        assertEquals(rdn.get("ou"), "Agents, Secret");

        rdn = dn.getRdn(2);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "Example");

        rdn = dn.getRdn(3);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "com");
    }

    public void testParsingCompositeRdn() throws Exception {
        DN dn = new DN("cn=James Bond+uid=jbond+displayName=007,ou=Users+description=Secret Agents,dc=Example,dc=com");

        assertEquals(dn.getLength(), 4);

        RDN rdn = dn.getRdn(0);
        Collection names = rdn.getNames();
        assertEquals(names.size(), 3);
        assertTrue(names.contains("cn"));
        assertEquals(rdn.get("cn"), "James Bond");
        assertTrue(names.contains("uid"));
        assertEquals(rdn.get("uid"), "jbond");
        assertTrue(names.contains("displayName"));
        assertEquals(rdn.get("displayName"), "007");

        rdn = dn.getRdn(1);
        names = rdn.getNames();
        assertEquals(names.size(), 2);
        assertTrue(names.contains("ou"));
        assertEquals(rdn.get("ou"), "Users");
        assertTrue(names.contains("description"));
        assertEquals(rdn.get("description"), "Secret Agents");

        rdn = dn.getRdn(2);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "Example");

        rdn = dn.getRdn(3);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "com");
    }

    public void testPattern() throws Exception {
        DN dn = new DN("cn=...+uid=...,ou=...,dc=Example,dc=com");
        String pattern1 = "cn={0}+uid={1},ou={2},dc=Example,dc=com";

        String pattern2 = dn.getPattern();

        log.debug("Pattern 1: "+pattern1);
        log.debug("Pattern 2: "+pattern2);
        assertEquals(pattern1, pattern2);
    }

}
