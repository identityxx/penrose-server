package org.safehaus.penrose.mapping;

import org.safehaus.penrose.ldap.DN;

/**
 * @author Endi S. Dewata
 */
public class Link {

    private String partitionName;
    private DN dn;

    public Link() {
    }

    public Link(Link link) {
        copy(link);
    }
    
    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
    }
    
    public void setDn(DN dn) {
        this.dn = dn;
    }

    public int hashCode() {
        return (partitionName == null ? 0 : partitionName.hashCode()) +
                (dn == null ? 0 : dn.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (object == this) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Link link = (Link)object;
        if (!equals(partitionName, link.partitionName)) return false;
        if (!equals(dn, link.dn)) return false;

        return true;
    }

    public void copy(Link link) {
        partitionName = link.partitionName;
        dn = link.dn;
    }

    public Object clone() throws CloneNotSupportedException {
        super.clone();
        return new Link(this);
    }
}
