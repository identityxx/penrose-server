package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class ModRdnRequest extends Request implements Cloneable {

    protected DN dn;
    protected RDN newRdn;
    protected boolean deleteOldRdn;

    public ModRdnRequest() {
    }

    public ModRdnRequest(ModRdnRequest request) {
        super(request);
        dn = request.getDn();
        newRdn = request.getNewRdn();
        deleteOldRdn = request.getDeleteOldRdn();
    }

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

    public void setNewRdn(String rdn) throws Exception {
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

    public Object clone() throws CloneNotSupportedException {
        ModRdnRequest request = (ModRdnRequest)super.clone();

        request.dn = dn;
        request.newRdn = newRdn;
        request.deleteOldRdn = deleteOldRdn;

        return request;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("dn: ");
        sb.append(dn);
        sb.append("\n");

        sb.append("changetype: modrdn\n");

        sb.append("newRdn: ");
        sb.append(newRdn);
        sb.append("\n");
        sb.append("deleteOldRdn: ");
        sb.append(deleteOldRdn ? 1 : 0);
        sb.append("\n");

        return sb.toString();
    }
}
