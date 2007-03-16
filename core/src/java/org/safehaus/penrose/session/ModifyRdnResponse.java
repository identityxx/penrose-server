package org.safehaus.penrose.session;

import org.safehaus.penrose.entry.DN;

/**
 * @author Endi S. Dewata
 */
public class ModifyRdnResponse extends Response {

    protected DN dn;

    public DN getDn() {
        return dn;
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }
}
