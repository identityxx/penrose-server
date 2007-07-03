package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.AddResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseAddResponse
        extends PenroseResponse
        implements com.identyx.javabackend.AddResponse {

    AddResponse addResponse;

    public PenroseAddResponse(AddResponse addResponse) {
        super(addResponse);
        this.addResponse = addResponse;
    }

    public AddResponse getAddResponse() {
        return addResponse;
    }
}
