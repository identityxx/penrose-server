package org.safehaus.penrose.test.quick.parser;

import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.mapping.Row;

import java.util.Collection;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class DnParserTest extends TestCase {

    public void testGetRdn() {
        String dn = "cn=James Bond,ou=Users,dc=Example,dc=com";
        Row rdn = EntryUtil.getRdn(dn);

        assertEquals(rdn.getNames().size(), 1);

        String cn = (String)rdn.getNames().iterator().next();
        assertEquals(cn, "cn");

        String value = (String)rdn.get(cn);
        assertEquals(value, "James Bond");
    }

    public void testGetParentDn() {
        String dn = "cn=James Bond,ou=Users,dc=Example,dc=com";
        String parentDn = EntryUtil.getParentDn(dn);

        assertEquals(parentDn, "ou=Users,dc=Example,dc=com");
    }

    public void testRdnWriter() {
        Row rdn = new Row();
        rdn.set("cn", "Bond, James");
        rdn.set("description", "Secret, Agent");

        assertEquals(EntryUtil.toString(rdn), "cn=Bond\\, James+description=Secret\\, Agent");
    }

    public void testParsingSimpleDn() {
        String dn = "cn=James Bond,ou=Users,dc=Example,dc=com";
        List rdns = EntryUtil.parseDn(dn);

        assertEquals(rdns.size(), 4);

        Row rdn = (Row)rdns.get(0);
        Collection names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("cn"));
        assertEquals(rdn.get("cn"), "James Bond");

        rdn = (Row)rdns.get(1);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("ou"));
        assertEquals(rdn.get("ou"), "Users");

        rdn = (Row)rdns.get(2);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "Example");

        rdn = (Row)rdns.get(3);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "com");

    }

    public void testParsingDnWithEscapedCharacters() {
        String dn = "cn=Bond\\, James,ou=Agents\\, Secret,dc=Example,dc=com";
        List rdns = EntryUtil.parseDn(dn);

        assertEquals(rdns.size(), 4);

        Row rdn = (Row)rdns.get(0);
        Collection names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("cn"));
        assertEquals(rdn.get("cn"), "Bond, James");

        rdn = (Row)rdns.get(1);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("ou"));
        assertEquals(rdn.get("ou"), "Agents, Secret");

        rdn = (Row)rdns.get(2);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "Example");

        rdn = (Row)rdns.get(3);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "com");

    }

    public void testParsingCompositeRdn() {
        String dn = "cn=James Bond+uid=jbond+displayName=007,ou=Users+description=Secret Agents,dc=Example,dc=com";
        List rdns = EntryUtil.parseDn(dn);

        assertEquals(rdns.size(), 4);

        Row rdn = (Row)rdns.get(0);
        Collection names = rdn.getNames();
        assertEquals(names.size(), 3);
        assertTrue(names.contains("cn"));
        assertEquals(rdn.get("cn"), "James Bond");
        assertTrue(names.contains("uid"));
        assertEquals(rdn.get("uid"), "jbond");
        assertTrue(names.contains("displayName"));
        assertEquals(rdn.get("displayName"), "007");

        rdn = (Row)rdns.get(1);
        names = rdn.getNames();
        assertEquals(names.size(), 2);
        assertTrue(names.contains("ou"));
        assertEquals(rdn.get("ou"), "Users");
        assertTrue(names.contains("description"));
        assertEquals(rdn.get("description"), "Secret Agents");

        rdn = (Row)rdns.get(2);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "Example");

        rdn = (Row)rdns.get(3);
        names = rdn.getNames();
        assertEquals(names.size(), 1);
        assertTrue(names.contains("dc"));
        assertEquals(rdn.get("dc"), "com");
    }
}
