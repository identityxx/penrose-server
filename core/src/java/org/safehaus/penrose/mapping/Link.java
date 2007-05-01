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
        this.partitionName = link.partitionName;
        this.dn = link.dn;
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
}
