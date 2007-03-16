package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.ModifyResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseModifyResponse
        extends PenroseResponse
        implements com.identyx.javabackend.ModifyResponse {

    ModifyResponse modifyResponse;

    public PenroseModifyResponse(ModifyResponse modifyResponse) {
        super(modifyResponse);
        this.modifyResponse = modifyResponse;
    }

    public ModifyResponse getModifyResponse() {
        return modifyResponse;
    }
}
