package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.BindResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseBindResponse
        extends PenroseResponse
        implements com.identyx.javabackend.BindResponse {

    BindResponse bindResponse;

    public PenroseBindResponse(BindResponse bindResponse) {
        super(bindResponse);
        this.bindResponse = bindResponse;
    }

    public BindResponse getBindResponse() {
        return bindResponse;
    }
}
