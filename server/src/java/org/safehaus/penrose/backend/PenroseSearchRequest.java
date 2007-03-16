package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.SearchRequest;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

import com.identyx.javabackend.Control;
import com.identyx.javabackend.DN;
import com.identyx.javabackend.Filter;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchRequest
        extends PenroseRequest
        implements com.identyx.javabackend.SearchRequest {

    SearchRequest searchRequest;

    public PenroseSearchRequest(SearchRequest searchRequest) {
        super(searchRequest);
        this.searchRequest = searchRequest;
    }

    public void setDn(DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        searchRequest.setDn(penroseDn.getDn());
    }

    public DN getDn() throws Exception {
        return new PenroseDN(searchRequest.getDn());
    }

    public void setFilter(Filter filter) throws Exception {
        PenroseFilter penroseFilter = (PenroseFilter)filter;
        searchRequest.setFilter(penroseFilter.getFilter());
    }

    public Filter getFilter() throws Exception {
        return new PenroseFilter(searchRequest.getFilter());
    }

    public void setScope(int scope) throws Exception {
        searchRequest.setScope(scope);
    }

    public int getScope() throws Exception {
        return searchRequest.getScope();
    }

    public void setTimeLimit(long timeLimit) throws Exception {
        searchRequest.setTimeLimit(timeLimit);
    }

    public long getTimeLimit() throws Exception {
        return searchRequest.getTimeLimit();
    }

    public void setSizeLimit(long sizeLimit) throws Exception {
        searchRequest.setSizeLimit(sizeLimit);
    }

    public long getSizeLimit() throws Exception {
        return searchRequest.getSizeLimit();
    }

    public void setAttributes(Collection attributes) throws Exception {
        searchRequest.setAttributes(attributes);
    }

    public Collection getAttributes() throws Exception {
        return searchRequest.getAttributes();
    }

    public SearchRequest getSearchRequest() throws Exception {
        return searchRequest;
    }
}
