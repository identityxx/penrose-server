package org.safehaus.penrose.mina;

import org.apache.directory.shared.ldap.name.LdapDN;

/**
 * @author Endi S. Dewata
 */
public class PenroseDN extends LdapDN {

    public PenroseDN(String dn) throws Exception {
        bytes = dn.getBytes();
    }
}
