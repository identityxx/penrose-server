package org.safehaus.penrose.ldap;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.RDN;

/**
 * @author Endi S. Dewata
 */
public class ModRdnRequest extends Request {

    protected DN dn;
    protected RDN newRdn;
    protected boolean deleteOldRdn;

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

    public RDN getNewRdn() {
        return newRdn;
    }

    public void setNewRdn(String rdn) {
        this.newRdn = new RDN(rdn);
    }

    public void setNewRdn(RDN newRdn) {
        this.newRdn = newRdn;
    }

    public boolean getDeleteOldRdn() {
        return deleteOldRdn;
    }

    public void setDeleteOldRdn(boolean deleteOldRdn) {
        this.deleteOldRdn = deleteOldRdn;
    }
}
