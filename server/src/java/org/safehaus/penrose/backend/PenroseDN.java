package org.safehaus.penrose.backend;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.RDN;

/**
 * @author Endi S. Dewata
 */
public class PenroseDN implements org.safehaus.penrose.ldapbackend.DN {

    DN dn;

    public PenroseDN(DN dn) {
        this.dn = dn;
    }

    public org.safehaus.penrose.ldapbackend.RDN getRdn() throws Exception {
        return new PenroseRDN(dn.getRdn());
    }

    public Collection<org.safehaus.penrose.ldapbackend.RDN> getRdns() throws Exception {
        List<org.safehaus.penrose.ldapbackend.RDN> rdns = new ArrayList<org.safehaus.penrose.ldapbackend.RDN>();
        for (RDN rdn : dn.getRdns()) {
            rdns.add(new PenroseRDN(rdn));
        }
        return rdns;
    }

    public DN getDn() {
        return dn;
    }

    public String toString() {
        return dn == null ? "" : dn.toString();
    }
}
