package org.safehaus.penrose.entry;

import java.util.*;
import java.text.MessageFormat;

/**
 * @author Endi S. Dewata
 */
public class DN implements Cloneable {

    public List rdns;

    public String originalDn;
    public String normalizedDn;
    public DN parentDn;

    public String pattern;
    public MessageFormat formatter;
    
    public DN() {
        rdns = new ArrayList();
    }

    public DN(String dn) {
        originalDn = dn;
    }

    public void parse() {
        if (rdns != null) return;
        rdns = new ArrayList();
        Collection list = DNBuilder.parse(originalDn);
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            rdns.add(rdn);
        }
    }

    public String getPattern() {
        if (pattern != null) return pattern;

        parse();
        StringBuilder sb = new StringBuilder();
        int counter = 0;

        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();

            counter = rdn.createPattern(counter);

            if (sb.length() > 0) sb.append(',');
            sb.append(rdn.getPattern());
        }

        pattern = sb.toString();
        
        return pattern;
    }

    public String format(Collection args) {
        if (formatter == null) {
            formatter = new MessageFormat(getPattern());
        }

        return formatter.format(args.toArray());
    }

    public boolean isEmpty() {
        if (originalDn == null) {
            return rdns.isEmpty();
        } else {
            return "".equals(originalDn);
        }
    }

    public int getSize() {
        parse();
        return rdns.size();
    }

    public RDN getRdn() {
        parse();
        if (rdns.size() == 0) return null;
        return (RDN)rdns.get(0);
    }

    public RDN get(int i) {
        parse();
        return (RDN)rdns.get(i);
    }

    public Collection getRdns() {
        parse();
        return rdns;
    }

    public String getOriginalDn() {
        if (originalDn != null) return originalDn;

        StringBuilder sb = new StringBuilder();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();

            if (sb.length() > 0) sb.append(",");
            sb.append(rdn.getOriginal());
        }

        originalDn = sb.toString();
        return originalDn;
    }

    public String getNormalizedDn() {
        if (normalizedDn != null) return normalizedDn;

        parse();
        StringBuilder sb = new StringBuilder();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();

            if (sb.length() > 0) sb.append(",");
            sb.append(rdn.getNormalized());
        }

        normalizedDn = sb.toString();
        return normalizedDn;
    }

    public DN getParentDn() {
        if (parentDn != null) return parentDn;

        parse();
        DNBuilder db = new DNBuilder();
        for (int i=1; i<rdns.size(); i++) {
            RDN rdn = (RDN)rdns.get(i);
            db.append(rdn);
        }

        parentDn = db.toDn();
        return parentDn;
    }

    public boolean endsWith(String suffix) {
        return endsWith(new DN(suffix));
    }
    
    public boolean endsWith(DN suffix) {
        parse();
        suffix.parse();
        int i1 = rdns.size();
        int i2 = suffix.rdns.size();

        if (i1 < i2) return false;

        while (i1 > 0 && i2 > 0) {
            RDN rdn1 = (RDN)rdns.get(i1-1);
            RDN rdn2 = (RDN)suffix.rdns.get(i2-1);

            if (!rdn1.match(rdn2)) return false;

            i1--;
            i2--;
        }

        return true;
    }

    public boolean matches(String dn) {
        return matches(new DN(dn));
    }
    
    public boolean matches(DN dn) {

        if (dn == null) return false;
        if (getNormalizedDn().equals(dn.getNormalizedDn())) return true;

        parse();
        dn.parse();
        if (rdns.size() != dn.rdns.size()) return false;

        Iterator i = rdns.iterator();
        Iterator j = dn.rdns.iterator();

        while (i.hasNext() && j.hasNext()) {
            RDN rdn1 = (RDN)i.next();
            RDN rdn2 = (RDN)j.next();

            if (!rdn1.match(rdn2)) return false;
        }

        return true;
    }

    public int hashCode() {
        return getOriginalDn().hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        DN dn = (DN)object;
        if (!equals(getOriginalDn(), dn.getOriginalDn())) return false;

        return true;
    }

    void copy(DN dn) {

        if (dn == null) return;

        if (dn.rdns == null) {
            rdns = null;

        } else {
            rdns = new ArrayList();
            for (Iterator i=dn.rdns.iterator(); i.hasNext(); ) {
                RDN rdn = (RDN)i.next();
                rdns.add(rdn);
            }
        }

        originalDn = dn.originalDn;
        normalizedDn = dn.normalizedDn;
        parentDn = dn.parentDn;

        pattern = dn.pattern;
        formatter = dn.formatter;
    }

    public Object clone() {
        DN dn = new DN();
        dn.copy(this);
        return dn;
    }

    public String toString() {
        return getOriginalDn();
    }
}
