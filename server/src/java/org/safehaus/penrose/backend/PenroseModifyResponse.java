package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.ModifyResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseModifyResponse
        extends PenroseResponse
        implements org.safehaus.penrose.ldapbackend.ModifyResponse {

    ModifyResponse modifyResponse;

    public PenroseModifyResponse(ModifyResponse modifyResponse) {
        super(modifyResponse);
        this.modifyResponse = modifyResponse;
    }

    public ModifyResponse getModifyResponse() {
        return modifyResponse;
    }
}
