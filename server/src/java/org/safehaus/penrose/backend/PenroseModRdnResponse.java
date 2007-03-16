package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.ModRdnResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseModRdnResponse
        extends PenroseResponse
        implements com.identyx.javabackend.ModRdnResponse {

    ModRdnResponse modRdnResponse;

    public PenroseModRdnResponse(ModRdnResponse modRdnResponse) {
        super(modRdnResponse);
        this.modRdnResponse = modRdnResponse;
    }

    public ModRdnResponse getModRdnResponse() {
        return modRdnResponse;
    }
}
