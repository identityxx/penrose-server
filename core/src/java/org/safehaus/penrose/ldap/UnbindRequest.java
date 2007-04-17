package org.safehaus.penrose.ldap;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.RDN;

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
    
    public void setDn(RDN rdn) throws Exception {
        this.dn = new DN(rdn);
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }
}
