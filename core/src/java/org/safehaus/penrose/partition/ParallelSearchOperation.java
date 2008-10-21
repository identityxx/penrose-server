package org.safehaus.penrose.partition;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.SearchOperation;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ParallelSearchOperation extends SearchOperation {

    public int counter;
    public Set<DN> dns = new HashSet<DN>();

    public ParallelSearchOperation(SearchOperation parent, int counter) {
        super(parent);

        //log.debug("Counter = "+counter);
        this.counter = counter;
    }

    public void add(SearchResult result) throws Exception {

        DN dn = result.getDn();
        if (dns.contains(dn)) return;

        dns.add(dn);
        super.add(result);
    }

    public void setException(LDAPException exception) {
        if (getReturnCode() == LDAP.SUCCESS) super.setException(exception);
    }

    public synchronized void close() throws Exception {

        if (counter > 0) counter--;
        //log.debug("Counter = "+counter);

        if (counter > 0) return;

        super.close();
        notifyAll();
    }

    public synchronized int waitFor() {
        while (counter > 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return getReturnCode();
    }
}