package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class BindRequest extends Request implements Cloneable {

    protected DN dn;
    protected byte[] password;

    public BindRequest() {
    }

    public BindRequest(BindRequest request) {
        super(request);
        dn = request.dn;
        password = request.password.clone();
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

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password == null ? null : password.getBytes();
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public Object clone() throws CloneNotSupportedException {
        BindRequest request = (BindRequest)super.clone();

        request.dn = dn;
        request.password = password;

        return request;
    }
}
