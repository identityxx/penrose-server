package org.safehaus.penrose.ldap;

import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class BindRequest extends Request implements Cloneable {

    protected DN dn;
    protected byte[] password;

    public BindRequest() {
    }

    public BindRequest(BindRequest request) throws Exception {
        super(request);
        copy(request);
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

    public int hashCode() {
        return controls.hashCode();
    }

    private boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        BindRequest request = (BindRequest)object;
        if (!equals(dn, request.dn)) return false;
        if (!Arrays.equals(password, request.password)) return false;
        if (!equals(controls, request.controls)) return false;

        return true;
    }

    public void copy(BindRequest request) throws CloneNotSupportedException {
        dn = request.dn;
        password = request.password.clone();
    }

    public Object clone() throws CloneNotSupportedException {
        BindRequest request = (BindRequest)super.clone();
        request.copy(this);
        return request;
    }
}
