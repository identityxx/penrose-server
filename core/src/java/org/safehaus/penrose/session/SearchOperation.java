package org.safehaus.penrose.session;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchReference;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SearchOperation extends Operation {

    public SearchOperation() {
    }

    public SearchOperation(SearchOperation parent) {
        super(parent);

        if (debug) {
            log.debug("Search Operation:");

            SearchRequest request = (SearchRequest)parent.getRequest();
            log.debug(" - DN: "+request.getDn());
        }
    }

    public DN getDn() {
        return ((SearchRequest)getRequest()).getDn();
    }

    public void setDn(DN dn) {
        ((SearchRequest)getRequest()).setDn(dn);
    }

    public int getScope() {
        return ((SearchRequest)getRequest()).getScope();
    }

    public void setScope(int scope) {
        ((SearchRequest)getRequest()).setScope(scope);
    }

    public Filter getFilter() {
        return ((SearchRequest)getRequest()).getFilter();
    }

    public void setFilter(Filter filter) {
        ((SearchRequest)getRequest()).setFilter(filter);
    }

    public Collection<String> getAttributes() {
        return ((SearchRequest)getRequest()).getAttributes();
    }

    public void setAttributes(Collection<String> attributes) {
        ((SearchRequest)getRequest()).setAttributes(attributes);
    }

    public long getSizeLimit() {
        return ((SearchRequest)getRequest()).getSizeLimit();
    }

    public void normalize() throws Exception {
        
        SchemaManager schemaManager = getSession().getPenroseContext().getSchemaManager();

        DN dn = getDn();
        DN normalizedDn = schemaManager.normalize(dn);
        setDn(normalizedDn);

        Collection<String> attributes = getAttributes();
        Collection<String> normalizedAttributes = schemaManager.normalize(attributes);
        setAttributes(normalizedAttributes);
    }

    public void add(SearchResult result) throws Exception {
        if (isAbandoned()) return;
        ((SearchResponse)getResponse()).add(result);
    }

    public void add(SearchReference reference) throws Exception {
        if (isAbandoned()) return;
        ((SearchResponse)getResponse()).add(reference);
    }

    public void close() throws Exception {
        ((SearchResponse)getResponse()).close();
    }

    public boolean isClosed() {
        return ((SearchResponse)getResponse()).isClosed();
    }

    public long getTotalCount() {
        return ((SearchResponse)getResponse()).getTotalCount();
    }

    public void setBufferSize(long bufferSize) {
        ((SearchResponse)getResponse()).setBufferSize(bufferSize);
    }

    public long getBufferSize() {
        return ((SearchResponse)getResponse()).getBufferSize();
    }
}
