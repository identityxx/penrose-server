package org.safehaus.penrose.backend;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.entry.RDN;

/**
 * @author Endi S. Dewata
 */
public class PenroseDN implements com.identyx.javabackend.DN {

    String dn;

    public PenroseDN(String dn) {
        this.dn = dn;
    }

    public com.identyx.javabackend.RDN getRdn() throws Exception {
        return new PenroseRDN(EntryUtil.getRdn(dn));
    }

    public Collection getRdns() throws Exception {
        List rdns = new ArrayList();
        for (Iterator i=EntryUtil.parseDn(dn).iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            rdns.add(new PenroseRDN(rdn));
        }
        return rdns;
    }

    public String toString() {
        return dn;
    }
}
