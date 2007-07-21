package org.safehaus.penrose.adapter.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.jdbc.Request;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class RequestBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected List<Request> requests = new ArrayList<Request>();

    public Collection<Request> getRequests() {
        return requests;
    }

    public void setRequests(Collection<Request> requests) {
        this.requests.clear();
        this.requests.addAll(requests);
    }
}
