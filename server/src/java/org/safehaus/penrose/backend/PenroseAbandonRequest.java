package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.AbandonRequest;

/**
 * @author Endi S. Dewata
 */
public class PenroseAbandonRequest
        extends PenroseRequest
        implements org.safehaus.penrose.ldapbackend.AbandonRequest {

    AbandonRequest abandonRequest;

    public PenroseAbandonRequest(AbandonRequest addRequest) {
        super(addRequest);
        this.abandonRequest = addRequest;
    }

    public void setIdToAbandon(int idToAbandon) throws Exception {
        abandonRequest.setIdToAbandon(idToAbandon);
    }

    public int getIdToAbandon() throws Exception {
        return abandonRequest.getIdToAbandon();
    }

    public AbandonRequest getAbandonRequest() {
        return abandonRequest;
    }
}