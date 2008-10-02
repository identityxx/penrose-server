package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.BindRequest;
import org.safehaus.penrose.ldapbackend.DN;

/**
 * @author Endi S. Dewata
 */
public class PenroseBindRequest
        extends PenroseRequest
        implements org.safehaus.penrose.ldapbackend.BindRequest {

    BindRequest bindRequest;

    public PenroseBindRequest(BindRequest bindRequest) {
        super(bindRequest);
        this.bindRequest = bindRequest;
    }

    public void setDn(DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        bindRequest.setDn(penroseDn.getDn());
    }

    public DN getDn() throws Exception {
        return new PenroseDN(bindRequest.getDn());
    }

    public void setPassword(String password) throws Exception {
        bindRequest.setPassword(password);
    }

    public void setPassword(byte[] password) throws Exception {
        bindRequest.setPassword(password);
    }

    public byte[] getPassword() throws Exception {
        return bindRequest.getPassword();
    }

    public BindRequest getBindRequest() {
        return bindRequest;
    }
}
