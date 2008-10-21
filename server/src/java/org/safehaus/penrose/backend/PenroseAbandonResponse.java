package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.AbandonResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseAbandonResponse
        extends PenroseResponse
        implements org.safehaus.penrose.ldapbackend.AbandonResponse {

    AbandonResponse abandonResponse;

    public PenroseAbandonResponse(AbandonResponse addResponse) {
        super(addResponse);
        this.abandonResponse = addResponse;
    }

    public AbandonResponse getAbandonResponse() {
        return abandonResponse;
    }
}