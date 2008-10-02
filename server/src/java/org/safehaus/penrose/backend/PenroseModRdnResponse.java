package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.ModRdnResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseModRdnResponse
        extends PenroseResponse
        implements org.safehaus.penrose.ldapbackend.ModRdnResponse {

    ModRdnResponse modRdnResponse;

    public PenroseModRdnResponse(ModRdnResponse modRdnResponse) {
        super(modRdnResponse);
        this.modRdnResponse = modRdnResponse;
    }

    public ModRdnResponse getModRdnResponse() {
        return modRdnResponse;
    }
}
