package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.CompareRequest;
import com.identyx.javabackend.DN;

/**
 * @author Endi S. Dewata
 */
public class PenroseCompareRequest
        extends PenroseRequest
        implements com.identyx.javabackend.CompareRequest {

    CompareRequest compareRequest;

    public PenroseCompareRequest(CompareRequest compareRequest) {
        super(compareRequest);
        this.compareRequest = compareRequest;
    }

    public void setDn(DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        compareRequest.setDn(penroseDn.getDn());
    }

    public DN getDn() throws Exception {
        return new PenroseDN(compareRequest.getDn());
    }

    public void setAttributeName(String name) throws Exception {
        compareRequest.setAttributeName(name);
    }

    public String getAttributeName() throws Exception {
        return compareRequest.getAttributeName();
    }

    public void setAttributeValue(Object value) throws Exception {
        compareRequest.setAttributeValue(value);
    }

    public Object getAttributeValue() throws Exception {
        return compareRequest.getAttributeValue();
    }

    public CompareRequest getCompareRequest() {
        return compareRequest;
    }
}
