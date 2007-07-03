package org.safehaus.penrose.backend;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.RDN;

/**
 * @author Endi S. Dewata
 */
public class PenroseDN implements com.identyx.javabackend.DN {

    DN dn;

    public PenroseDN(DN dn) {
        this.dn = dn;
    }

    public com.identyx.javabackend.RDN getRdn() throws Exception {
        return new PenroseRDN(dn.getRdn());
    }

    public Collection<com.identyx.javabackend.RDN> getRdns() throws Exception {
        List<com.identyx.javabackend.RDN> rdns = new ArrayList<com.identyx.javabackend.RDN>();
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
