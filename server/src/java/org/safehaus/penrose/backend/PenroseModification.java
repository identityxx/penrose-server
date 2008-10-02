package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Modification;
import org.safehaus.penrose.ldapbackend.Attribute;

/**
 * @author Endi S. Dewata
 */
public class PenroseModification implements org.safehaus.penrose.ldapbackend.Modification {

    Modification modification;

    public PenroseModification(Modification modification) throws Exception {
        this.modification = modification;
    }
    
    public PenroseModification(int type, Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        this.modification = new Modification(type, penroseAttribute.getAttribute());
    }

    public void setType(int type) throws Exception {
        modification.setType(type);
    }

    public int getType() throws Exception {
        return modification.getType();
    }

    public void setAttribute(Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        modification.setAttribute(penroseAttribute.getAttribute());
    }

    public Attribute getAttribute() throws Exception {
        return new PenroseAttribute(modification.getAttribute());
    }

    public Modification getModification() throws Exception {
        return modification;
    }
}
