package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.DeleteResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseDeleteResponse
        extends PenroseResponse
        implements com.identyx.javabackend.DeleteResponse {

    DeleteResponse deleteResponse;

    public PenroseDeleteResponse(DeleteResponse deleteResponse) {
        super(deleteResponse);
        this.deleteResponse = deleteResponse;
    }

    public DeleteResponse getDeleteResponse() {
        return deleteResponse;
    }
}
