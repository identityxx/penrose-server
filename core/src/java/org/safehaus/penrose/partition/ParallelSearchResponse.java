package org.safehaus.penrose.partition;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.Penrose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ParallelSearchResponse extends Pipeline {

    public Logger log = LoggerFactory.getLogger(getClass());

    int counter;
    Set<DN> dns = new HashSet<DN>();

    public ParallelSearchResponse(SearchResponse response, int counter) {
        super(response);

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
                Penrose.errorLog.error(e.getMessage(), e);
            }
        }
        return getReturnCode();
    }
}
