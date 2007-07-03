package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.UnbindRequest;
import com.identyx.javabackend.DN;

/**
 * @author Endi S. Dewata
 */
public class PenroseUnbindRequest
        extends PenroseRequest
        implements com.identyx.javabackend.UnbindRequest {

    UnbindRequest unbindRequest;

    public PenroseUnbindRequest(UnbindRequest unbindRequest) {
        super(unbindRequest);
        this.unbindRequest = unbindRequest;
    }

    public void setDn(DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        unbindRequest.setDn(penroseDn.getDn());
    }

    public DN getDn() throws Exception {
        return new PenroseDN(unbindRequest.getDn());
    }

    public UnbindRequest getUnbindRequest() {
        return unbindRequest;
    }
}
