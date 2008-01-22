package org.safehaus.penrose.control;

import java.util.Arrays;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Control implements Serializable, Cloneable {

    private String oid;
    private byte[] value;
    private boolean critical;

    public Control() {
    }

    public Control(String oid, byte[] value, boolean critical) {
        this.oid = oid;
        this.value = value;
        this.critical = critical;
    }
    
    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public boolean isCritical() {
        return critical;
    }

    public void setCritical(boolean critical) {
        this.critical = critical;
    }

    public String toString() {
        return oid;
    }

    public int hashCode() {
        return oid.hashCode();
    }

    private boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Control control = (Control)object;
        if (!equals(oid, control.oid)) return false;
        if (!Arrays.equals(value, control.value)) return false;
        if (!equals(critical, control.critical)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        Control control = (Control)super.clone();

        control.oid = oid;

        if (value != null) {
            control.value = new byte[value.length];
            System.arraycopy(value, 0, control.value, 0, value.length);
        }

        control.critical = critical;

        return control;
    }
}
