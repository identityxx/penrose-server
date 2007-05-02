package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class BindRequest extends Request {

    protected DN dn;
    protected String password;

    public BindRequest() {
    }

    public BindRequest(BindRequest request) {
        super(request);
        dn = request.getDn();
        password = request.getPassword();
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

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
