package org.safehaus.penrose.jdbc;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public abstract class Statement implements Serializable, Cloneable {

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
