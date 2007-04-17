package org.safehaus.penrose.adapter.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.DNBuilder;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.ldap.Request;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class RequestBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected String suffix;
    protected List<Request> requests = new ArrayList<Request>();

    public RequestBuilder() {
    }

    public DN getDn(Source source, RDN rdn) throws Exception {
        String baseDn = source.getParameter(LDAPAdapter.BASE_DN);

        DNBuilder db = new DNBuilder();
        db.append(rdn);
        db.append(baseDn);
        db.append(suffix);
        DN dn = db.toDn();

        return dn;
    }

    public Collection<Request> getRequests() {
        return requests;
    }

    public void setRequests(Collection<Request> requests) {
        this.requests.clear();
        this.requests.addAll(requests);
    }
}
