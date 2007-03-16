package org.safehaus.penrose.session;

import org.safehaus.penrose.entry.DN;

/**
 * @author Endi S. Dewata
 */
public class UnbindRequest extends Request {

    protected DN dn;

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
    }
    
    public void setDn(DN dn) {
        this.dn = dn;
    }
}
