package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.CompareResponse;

/**
 * @author Endi S. Dewata
 */
public class PenroseCompareResponse
        extends PenroseResponse
        implements com.identyx.javabackend.CompareResponse {

    CompareResponse compareResponse;

    public PenroseCompareResponse(CompareResponse compareResponse) {
        super(compareResponse);
        this.compareResponse = compareResponse;
    }

    public CompareResponse getCompareResponse() {
        return compareResponse;
    }
}
