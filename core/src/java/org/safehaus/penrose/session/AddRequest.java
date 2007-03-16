package org.safehaus.penrose.session;

import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Attributes;

/**
 * @author Endi S. Dewata
 */
public class AddRequest extends Request {

    protected DN dn;
    protected Attributes attributes;

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) throws Exception {
        this.dn = new DN(dn);
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }
}
