package org.safehaus.penrose.backend;

import org.safehaus.penrose.mapping.Row;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseRDN implements com.identyx.javabackend.RDN {

    Row rdn;

    public PenroseRDN(Row rdn) {
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
