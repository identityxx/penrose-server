package org.safehaus.penrose.control;

/**
 * @author Endi S. Dewata
 */
public class Control {

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
}
