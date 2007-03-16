package org.safehaus.penrose.session;

import org.safehaus.penrose.entry.DN;

/**
 * @author Endi S. Dewata
 */
public class BindRequest extends Request {

    protected DN dn;
    protected String password;

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
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
