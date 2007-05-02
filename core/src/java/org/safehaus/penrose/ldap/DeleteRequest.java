package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class DeleteRequest extends Request {

    protected DN dn;

    public DeleteRequest() {
    }

    public DeleteRequest(DeleteRequest request) {
        super(request);
        dn = request.getDn();
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
}
