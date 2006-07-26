package org.safehaus.penrose.parser;

import junit.framework.TestCase;
import org.ietf.ldap.LDAPDN;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class DnParserTest extends TestCase {

    Logger log = Logger.getLogger(getClass());

    public void testEscapeComma() {
        String rdn = "uid=Endi S. Dewata, Jr.";
        String newRdn = LDAPDN.escapeRDN(rdn);

        System.out.println("RDN: "+rdn);
        System.out.println("New RDN: "+newRdn);
    }

    public void testUnescapeComma() {
        String rdn = "uid=Endi S. Dewata\\, Jr.";
        String newRdn = LDAPDN.unescapeRDN(rdn);

        System.out.println("RDN: "+rdn);
        System.out.println("New RDN: "+newRdn);
    }
}
