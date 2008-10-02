package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.AddRequest;
import org.safehaus.penrose.ldapbackend.DN;
import org.safehaus.penrose.ldapbackend.Attributes;

/**
 * @author Endi S. Dewata
 */
public class PenroseAddRequest
        extends PenroseRequest
        implements org.safehaus.penrose.ldapbackend.AddRequest {

    AddRequest addRequest;

    public PenroseAddRequest(AddRequest addRequest) {
        super(addRequest);
        this.addRequest = addRequest;
    }

    public void setDn(DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        addRequest.setDn(penroseDn.getDn());
    }

    public DN getDn() throws Exception {
        return new PenroseDN(addRequest.getDn());
    }

    public void setAttributes(Attributes attributes) throws Exception {
        PenroseAttributes penroseAttributes = (PenroseAttributes)attributes;
        addRequest.setAttributes(penroseAttributes.getAttributes());
    }

    public Attributes getAttributes() throws Exception {
        return new PenroseAttributes(addRequest.getAttributes());
    }

    public AddRequest getAddRequest() {
        return addRequest;
    }
}
