package org.safehaus.penrose.backend;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.DN;

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
        return dn.getRdns();
    }

    public String toString() {
        return dn.toString();
    }
}
