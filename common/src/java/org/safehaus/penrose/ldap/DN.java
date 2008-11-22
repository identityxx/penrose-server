package org.safehaus.penrose.ldap;

import java.util.*;
import java.text.MessageFormat;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class DN implements Serializable, Comparable {

    public List<RDN> rdns;

    public String originalDn;
    public String normalizedDn;
    public DN parentDn;

    public String pattern;
    public transient MessageFormat formatter;
    
    public DN() {
        rdns = new ArrayList<RDN>();
    }

    public DN(String dn) {
        originalDn = dn;
    }

    public DN(RDN rdn) {
        rdns = new ArrayList<RDN>();
        rdns.add(rdn);
    }

    public synchronized void parse() {
        if (rdns != null) return;
        rdns = new ArrayList<RDN>();
        Collection<RDN> list = DNBuilder.parse(originalDn);
        for (RDN rdn : list) {
            rdns.add(rdn);
        }
    }

    public synchronized DN getDn(int start, int end) {
        parse();

        DNBuilder db = new DNBuilder();
        for (int i=start; i<end; i++) {
            db.append(rdns.get(i));
        }

        return db.toDn();
    }

    public synchronized DN append(String dn) throws Exception {
        return append(new DN(dn));
    }

    public synchronized DN append(RDN rdn) throws Exception {
        return append(new DN(rdn));
    }

    public synchronized DN append(DN dn) throws Exception {
        DNBuilder db = new DNBuilder();
        db.append(this);
        db.append(dn);
        return db.toDn();
    }

    public synchronized DN prepend(String dn) throws Exception {
        return prepend(new DN(dn));
    }
    
    public synchronized DN prepend(RDN rdn) throws Exception {
        return prepend(new DN(rdn));
    }

    public synchronized DN prepend(DN dn) throws Exception {
        DNBuilder db = new DNBuilder();
        db.append(dn);
        db.append(this);
        return db.toDn();
    }

    public synchronized DN getSuffix(int i) {
        return getDn(i, getSize());
    }

    public synchronized DN getPrefix(int i) throws Exception {
        return getDn(0, i);
    }

    public synchronized DN getPrefix(String suffix) throws Exception {
        return getPrefix(new DN(suffix));
    }

    public synchronized DN getPrefix(DN suffix) throws Exception {
        return getPrefix(getSize() - suffix.getSize());
    }

    public synchronized String getPattern() throws Exception {
        if (pattern != null) return pattern;

        parse();
        StringBuilder sb = new StringBuilder();
        int counter = 0;

        for (RDN rdn : rdns) {

            counter = rdn.createPattern(counter);

            if (sb.length() > 0) sb.append(',');
            sb.append(rdn.getPattern());
        }

        pattern = sb.toString();
        
        return pattern;
    }

    public synchronized String format(Collection<Object> args) throws Exception {
        if (formatter == null) {
            formatter = new MessageFormat(getPattern());
        }

        Collection<String> values = new ArrayList<String>();
        for (Object arg : args) {
            String value = arg.toString();
            values.add(LDAP.escape(value));
        }
        
        return formatter.format(values.toArray());
    }

    public synchronized boolean isEmpty() {
        if (originalDn == null) {
            return rdns == null || rdns.isEmpty();
        } else {
            return "".equals(originalDn);
        }
    }

    public synchronized int getSize() {
        parse();
        return rdns.size();
    }

    public synchronized RDN getRdn() {
        parse();
        if (rdns.size() == 0) return null;
        return rdns.get(0);
    }

    public synchronized RDN get(int i) {
        parse();
        return rdns.get(i);
    }

    public synchronized Collection<RDN> getRdns() {
        parse();
        return rdns;
    }

    public synchronized String getOriginalDn() throws Exception {
        if (originalDn != null) return originalDn;

        StringBuilder sb = new StringBuilder();

        if (rdns != null) {
            for (RDN rdn : rdns) {
                if (sb.length() > 0) sb.append(",");
                sb.append(rdn.getOriginal());
            }
        }

        originalDn = sb.toString();

        return originalDn;
    }

    public synchronized String getNormalizedDn() throws Exception {
        if (normalizedDn != null) return normalizedDn;

        parse();
        StringBuilder sb = new StringBuilder();
        for (RDN rdn : rdns) {

            if (sb.length() > 0) sb.append(",");
            sb.append(rdn.getNormalized());
        }

        normalizedDn = sb.toString();
        return normalizedDn;
    }

    public synchronized DN getParentDn() {
        if (parentDn != null) return parentDn;

        parse();
        DNBuilder db = new DNBuilder();
        for (int i=1; i<rdns.size(); i++) {
            RDN rdn = rdns.get(i);
            db.append(rdn);
        }

        parentDn = db.toDn();
        return parentDn;
    }

    public synchronized boolean endsWith(String suffix) throws Exception {
        return endsWith(new DN(suffix));
    }
    
    public synchronized boolean endsWith(DN suffix) throws Exception {
        parse();
        suffix.parse();
        int i1 = rdns.size();
        int i2 = suffix.rdns.size();

        if (i1 < i2) return false;

        while (i1 > 0 && i2 > 0) {
            RDN rdn1 = rdns.get(i1-1);
            RDN rdn2 = suffix.rdns.get(i2-1);

            if (!rdn1.matches(rdn2)) return false;

            i1--;
            i2--;
        }

        return true;
    }

    public synchronized boolean matches(String dn) throws Exception {
        return matches(new DN(dn));
    }
    
    public synchronized boolean matches(DN dn) throws Exception {

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

            if (!rdn1.matches(rdn2)) return false;
        }

        return true;
    }

    public synchronized int hashCode() {
        try {
            return getOriginalDn().hashCode();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    synchronized boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public synchronized boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;

        try {
            if (object instanceof String) {
                String dn = (String)object;
                return equals(getOriginalDn(), dn);
            }

            if (object instanceof DN) {
                DN dn = (DN)object;
                return equals(getOriginalDn(), dn.getOriginalDn());
            }

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return false;
    }

    public synchronized int compareTo(Object object) {

        if (object == null) return 0;
        if (!(object instanceof DN)) return 0;

        DN dn = (DN)object;

        if (rdns.size() < dn.rdns.size()) return -1;
        if (rdns.size() > dn.rdns.size()) return 1;

        int i = rdns.size();

        while (i > 0) {
            RDN rdn1 = rdns.get(i-1);
            RDN rdn2 = dn.rdns.get(i-1);

            int c = rdn1.compareTo(rdn2);
            if (c != 0) return c;

            i--;
        }

        return 0;
    }

    public synchronized String toString() {
        try {
            return getOriginalDn();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
