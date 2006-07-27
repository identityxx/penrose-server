package org.safehaus.penrose.parser;

import junit.framework.TestCase;
import org.ietf.ldap.LDAPDN;
import org.apache.log4j.Logger;
import org.safehaus.penrose.util.EntryUtil;

/**
 * @author Endi S. Dewata
 */
public class DnParserTest extends TestCase {

    Logger log = Logger.getLogger(getClass());

    public void testGetRdn() {
        String dn = "cn=James Bond,ou=Users,dc=Example,dc=com";
        String rdn = EntryUtil.getRdn(dn).toString();

        System.out.println("RDN: "+rdn);
    }

    public void testGetParentDn() {
        String dn = "cn=James Bond,ou=Users,dc=Example,dc=com";
        String parentDn = EntryUtil.getParentDn(dn);

        System.out.println("Parent DN: "+parentDn);
    }

    public void testEscapeRdn() {
        String rdn = "cn=James Bond, Jr.";
        String newRdn = LDAPDN.escapeRDN(rdn);

        System.out.println("RDN: "+rdn);
        System.out.println("New RDN: "+newRdn);
    }

    public void testUnescapeRdn() {
        String rdn = "cn=James Bond\\, Jr.";
        String newRdn = LDAPDN.unescapeRDN(rdn);

        System.out.println("RDN: "+rdn);
        System.out.println("New RDN: "+newRdn);
    }

    public void testExplodeDn() {
        String dn = "cn=James Bond,ou=Users,dc=Example,dc=com";
        String rdns[] = LDAPDN.explodeDN(dn, false);

        System.out.println("RDNs: ");
        for (int i=0; i<rdns.length; i++) {
            System.out.println(" - "+rdns[i]);
        }
    }

    public void testExplodeRdn() {
        String rdn = "cn=James Bond+uid=jbond+displayName=007";
        String attrs[] = LDAPDN.explodeRDN(rdn, false);

        System.out.println("Attributes: ");
        for (int i=0; i<attrs.length; i++) {
            System.out.println(" - "+attrs[i]);
        }
    }
}
