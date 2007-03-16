package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.ModifyRequest;
import com.identyx.javabackend.DN;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PenroseModifyRequest
        extends PenroseRequest
        implements com.identyx.javabackend.ModifyRequest {

    ModifyRequest modifyRequest;

    public PenroseModifyRequest(ModifyRequest modifyRequest) {
        super(modifyRequest);
        this.modifyRequest = modifyRequest;
    }

    public void setDn(String dn) throws Exception {
        modifyRequest.setDn(dn);
    }

    public void setDn(DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        modifyRequest.setDn(penroseDn.getDn());
    }

    public DN getDn() throws Exception {
        return new PenroseDN(modifyRequest.getDn());
    }

    public void setModifications(Collection modifications) throws Exception {
        modifyRequest.setModifications(modifications);
    }

    public Collection getModifications() throws Exception {
        return modifyRequest.getModifications();
    }
    
    public ModifyRequest getModifyRequest() {
        return modifyRequest;
    }
}
