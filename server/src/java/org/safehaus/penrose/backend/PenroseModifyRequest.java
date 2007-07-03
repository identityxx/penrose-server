package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.ModifyRequest;
import org.safehaus.penrose.ldap.Modification;
import com.identyx.javabackend.DN;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

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

    public void setDn(DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        modifyRequest.setDn(penroseDn.getDn());
    }

    public DN getDn() throws Exception {
        return new PenroseDN(modifyRequest.getDn());
    }

    public void setModifications(Collection modifications) throws Exception {
        Collection<Modification> list = new ArrayList<Modification>();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            PenroseModification modification = (PenroseModification)i.next();
            list.add(modification.getModification());
        }
        modifyRequest.setModifications(list);
    }

    public Collection getModifications() throws Exception {
        Collection list = new ArrayList();
        for (Iterator i=modifyRequest.getModifications().iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();
            list.add(new PenroseModification(modification));
        }
        return list;
    }
    
    public ModifyRequest getModifyRequest() {
        return modifyRequest;
    }
}
