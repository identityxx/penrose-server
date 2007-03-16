package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.UnbindResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseUnbindResponse
        extends PenroseResponse
        implements com.identyx.javabackend.UnbindResponse {

    UnbindResponse unbindResponse;

    public PenroseUnbindResponse(UnbindResponse unbindResponse) {
        super(unbindResponse);
        this.unbindResponse = unbindResponse;
    }

    public UnbindResponse getUnbindResponse() {
        return unbindResponse;
    }
}
