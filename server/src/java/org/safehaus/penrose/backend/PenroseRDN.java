package org.safehaus.penrose.backend;

import org.safehaus.penrose.entry.RDN;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseRDN implements com.identyx.javabackend.RDN {

    RDN rdn;

    public PenroseRDN(RDN rdn) {
        this.rdn = rdn;
    }

    public Collection getNames() throws Exception {
        return rdn.getNames();
    }

    public Collection getValues(String name) throws Exception {
        Collection values = new ArrayList();
        values.add(rdn.get(name));
        return values;
    }
}
