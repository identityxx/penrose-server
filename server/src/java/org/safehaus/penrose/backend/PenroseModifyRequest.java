package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.ModifyRequest;
import com.identyx.javabackend.DN;
import com.identyx.javabackend.Modification;
import com.identyx.javabackend.Attribute;

import javax.naming.directory.ModificationItem;
import javax.naming.NamingEnumeration;
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
        Collection list = new ArrayList();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            javax.naming.directory.Attribute attr = new javax.naming.directory.BasicAttribute(attribute.getName());
            for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
                Object value = j.next();
                attr.add(value);
            }
            ModificationItem mi = new ModificationItem(type, attr);
            list.add(mi);
        }
        modifyRequest.setModifications(list);
    }

    public Collection getModifications() throws Exception {
        Collection list = new ArrayList();
        for (Iterator i=modifyRequest.getModifications().iterator(); i.hasNext(); ) {
            ModificationItem mi = (ModificationItem)i.next();

            int type = mi.getModificationOp();
            javax.naming.directory.Attribute attribute = mi.getAttribute();

            Attribute attr = new PenroseAttribute(attribute.getID());
            for (NamingEnumeration ne = attribute.getAll(); ne.hasMore(); ) {
                Object value = ne.next();
                attr.addValue(value);
            }

            Modification modification = new PenroseModification(type, attr);
            list.add(modification);
        }
        return list;
    }
    
    public ModifyRequest getModifyRequest() {
        return modifyRequest;
    }
}
