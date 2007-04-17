package org.safehaus.penrose.ldap;

import org.safehaus.penrose.ldap.Attribute;

/**
 * @author Endi S. Dewata
 */
public class Modification {

    public final static int ADD     = 1;
    public final static int REPLACE = 2;
    public final static int DELETE  = 3;

    protected int type;
    protected Attribute attribute;

    public Modification(int type, Attribute attribute) throws Exception {
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
}
