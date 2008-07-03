package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class DeleteRequest extends Request implements Cloneable {

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

    public Object clone() throws CloneNotSupportedException {
        DeleteRequest request = (DeleteRequest)super.clone();

        request.dn = dn;

        return request;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("dn: ");
        sb.append(dn);
        sb.append("\n");

        sb.append("changetype: delete\n");

        return sb.toString();
    }
}
