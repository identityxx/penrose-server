package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.Modification;
import org.safehaus.penrose.entry.Attribute;

import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseModification implements com.identyx.javabackend.Modification {

    Modification modification;

    public PenroseModification(Modification modification) throws Exception {
        this.modification = modification;
    }
    
    public PenroseModification(int type, com.identyx.javabackend.Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        this.modification = new Modification(type, penroseAttribute.getAttribute());
    }

    public void setType(int type) throws Exception {
        modification.setType(type);
    }

    public int getType() throws Exception {
        return modification.getType();
    }

    public void setAttribute(com.identyx.javabackend.Attribute attribute) throws Exception {
        PenroseAttribute penroseAttribute = (PenroseAttribute)attribute;
        modification.setAttribute(penroseAttribute.getAttribute());
    }

    public com.identyx.javabackend.Attribute getAttribute() throws Exception {
        return new PenroseAttribute(modification.getAttribute());
    }

    public Modification getModification() throws Exception {
        return modification;
    }
}
