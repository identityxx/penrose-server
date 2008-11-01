package org.safehaus.penrose.partition;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.operation.PipelineSearchOperation;
import org.safehaus.penrose.directory.Entry;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ParallelSearchOperation extends PipelineSearchOperation {

    public int total;
    public int counter;
    public Set<DN> dns = new HashSet<DN>();

    public ParallelSearchOperation(SearchOperation parent, int total) {
        super(parent);

        this.total = total;

        if (debug) log.debug("Start searching "+total+" entries.");

        counter = total;
    }

    public void add(SearchResult result) throws Exception {

        //if (debug) log.debug("Result: \""+result.getDn()+"\".");

        DN dn = result.getDn();
        if (dns.contains(dn)) {
            if (debug) log.debug("Duplicate entry \""+result.getDn()+"\".");
            return;
        }

        dns.add(dn);
        super.add(result);
    }

    public void setException(LDAPException exception) {
        if (getReturnCode() == LDAP.SUCCESS) super.setException(exception);
    }

    public synchronized void close(Entry entry) throws Exception {

        if (debug) log.debug("Done searching \""+entry.getDn()+"\".");

        if (counter > 0) counter--;
        //log.debug("Counter = "+counter);

        if (counter > 0) {
            if (debug) log.debug("Waiting for "+counter+" more search operations.");
            return;
        }

        if (debug) log.debug("Done searching "+total+" entries.");
        close();
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