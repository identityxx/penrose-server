package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.RDN;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseRDN implements org.safehaus.penrose.ldapbackend.RDN {

    RDN rdn;

    public PenroseRDN(RDN rdn) {
        this.rdn = rdn;
    }

    public Collection<String> getNames() throws Exception {
        return rdn.getNames();
    }

    public Collection<Object> getValues(String name) throws Exception {
        Collection<Object> values = new ArrayList<Object>();
        values.add(rdn.get(name));
        return values;
    }

    public RDN getRdn() {
        return rdn;
    }
}
