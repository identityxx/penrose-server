package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.AddResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseAddResponse
        extends PenroseResponse
        implements org.safehaus.penrose.ldapbackend.AddResponse {

    AddResponse addResponse;

    public PenroseAddResponse(AddResponse addResponse) {
        super(addResponse);
        this.addResponse = addResponse;
    }

    public AddResponse getAddResponse() {
        return addResponse;
    }
}
