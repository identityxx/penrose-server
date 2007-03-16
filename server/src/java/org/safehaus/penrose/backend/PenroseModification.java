package org.safehaus.penrose.backend;

import com.identyx.javabackend.Modification;
import com.identyx.javabackend.Attribute;

import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseModification implements Modification {

    int type;
    Attribute attribute;

    public PenroseModification(int type, Attribute attribute) throws Exception {
        this.type = type;
        this.attribute = attribute;
    }

    public void setType(int type) throws Exception {
        this.type = type;
    }

    public int getType() throws Exception {
        return type;
    }

    public void setAttribute(Attribute attribute) throws Exception {
        this.attribute = attribute;
    }

    public Attribute getAttribute() throws Exception {
        return attribute;
    }

    public javax.naming.directory.ModificationItem getModificationItem() throws Exception {
        javax.naming.directory.Attribute attr = new javax.naming.directory.BasicAttribute(attribute.getName());
        for (Iterator i=attribute.getValues().iterator(); i.hasNext(); ) {
            Object value = i.next();
            attr.add(value);
        }
        return new javax.naming.directory.ModificationItem(type, attr);
    }
}
