package org.safehaus.penrose.ldap;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Modification implements Serializable, Cloneable {

    public final static int ADD     = 1;
    public final static int REPLACE = 2;
    public final static int DELETE  = 3;

    protected int type;
    protected Attribute attribute;

    public Modification(int type, Attribute attribute) {
        this.type = type;
        this.attribute = attribute;
    }

    public void setType(int type) throws Exception {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setAttribute(Attribute attribute) {
        this.attribute = attribute;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public Object clone() throws CloneNotSupportedException {
        Modification modification = (Modification)super.clone();

        modification.type = type;
        modification.attribute = (Attribute)attribute.clone();

        return modification;
    }
}
