package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;

/**
 * @author Endi S. Dewata
 */
public class PenroseEntry implements com.identyx.javabackend.Entry {

    public DN dn;
    public Attributes attributes;

    public PenroseEntry(DN dn, Attributes attributes) {
        this.dn = dn;
        this.attributes = attributes;
    }

    public com.identyx.javabackend.DN getDn() throws Exception {
        return new PenroseDN(dn);
    }

    public com.identyx.javabackend.Attributes getAttributes() throws Exception {
        return new PenroseAttributes(attributes);
    }
}
