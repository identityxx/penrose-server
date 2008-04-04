package org.safehaus.penrose.jdbc.connection;

import org.safehaus.penrose.jdbc.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class RequestBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected List<Statement> requests = new ArrayList<Statement>();

    public Collection<Statement> getRequests() {
        return requests;
    }

    public void setRequests(Collection<Statement> requests) {
        this.requests.clear();
        this.requests.addAll(requests);
    }
}
