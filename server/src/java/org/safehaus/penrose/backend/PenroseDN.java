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

    public Collection getRdns() throws Exception {
        List rdns = new ArrayList();
        for (Iterator i=dn.getRdns().iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            rdns.add(new PenroseRDN(rdn));
        }
        return rdns;
    }

    public DN getDn() {
        return dn;
    }

    public String toString() {
        return dn.toString();
    }
}
