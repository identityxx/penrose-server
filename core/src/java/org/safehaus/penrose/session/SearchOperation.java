package org.safehaus.penrose.session;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.filter.Filter;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SearchOperation extends Operation {

    public SearchOperation() {
    }

    public SearchOperation(SearchOperation parent) {
        super(parent);
    }

    public DN getDn() {
        if (parent == null) {
            return ((SearchRequest)request).getDn();
        } else {
            return ((SearchOperation)parent).getDn();
        }
    }

    public void setDn(DN dn) {
        if (parent == null) {
            ((SearchRequest)request).setDn(dn);
        } else {
            ((SearchOperation)parent).setDn(dn);
        }
    }

    public int getScope() {
        if (parent == null) {
            return ((SearchRequest)request).getScope();
        } else {
            return ((SearchOperation)parent).getScope();
        }
    }

    public void setScope(int scope) {
        if (parent == null) {
            ((SearchRequest)request).setScope(scope);
        } else {
            ((SearchOperation)parent).setScope(scope);
        }
    }

    public Filter getFilter() {
        if (parent == null) {
            return ((SearchRequest)request).getFilter();
        } else {
            return ((SearchOperation)parent).getFilter();
        }
    }

    public void setFilter(Filter filter) {
        if (parent == null) {
            ((SearchRequest)request).setFilter(filter);
        } else {
            ((SearchOperation)parent).setFilter(filter);
        }
    }

    public Collection<String> getAttributes() {
        if (parent == null) {
            return ((SearchRequest)request).getAttributes();
        } else {
            return ((SearchOperation)parent).getAttributes();
        }
    }

    public void setAttributes(Collection<String> attributes) {
        if (parent == null) {
            ((SearchRequest)request).setAttributes(attributes);
        } else {
            ((SearchOperation)parent).setAttributes(attributes);
        }
    }

    public long getSizeLimit() {
        if (parent == null) {
            return ((SearchRequest)request).getSizeLimit();
        } else {
            return ((SearchOperation)parent).getSizeLimit();
        }
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

        if (isAbandoned()) {
            if (debug) log.debug("Operation "+getOperationName()+" has been abandoned.");
            return;
        }

        long sizeLimit = getSizeLimit();
        long totalCount = getTotalCount();

        if (sizeLimit > 0 && totalCount >= sizeLimit) {
            LDAPException exception = LDAP.createException(LDAP.SIZE_LIMIT_EXCEEDED);
            setException(exception);
            throw exception;
        }

        if (parent == null) {
            ((SearchResponse)response).add(result);
        } else {
            ((SearchOperation)parent).add(result);
        }
    }

    public void add(SearchReference reference) throws Exception {

        if (isAbandoned()) {
            if (debug) log.debug("Operation "+getOperationName()+" has been abandoned.");
            return;
        }

        if (parent == null) {
            ((SearchResponse)response).add(reference);
        } else {
            ((SearchOperation)parent).add(reference);
        }
    }

    public void close() throws Exception {
        if (parent == null) {
            ((SearchResponse)response).close();
        } else {
            ((SearchOperation)parent).close();
        }
    }

    public boolean isClosed() {
        if (parent == null) {
            return ((SearchResponse)response).isClosed();
        } else {
            return ((SearchOperation)parent).isClosed();
        }
    }

    public long getTotalCount() {
        if (parent == null) {
            return ((SearchResponse)response).getTotalCount();
        } else {
            return ((SearchOperation)parent).getTotalCount();
        }
    }

    public void setBufferSize(long bufferSize) {
        if (parent == null) {
            ((SearchResponse)response).setBufferSize(bufferSize);
        } else {
            ((SearchOperation)parent).setBufferSize(bufferSize);
        }
    }

    public long getBufferSize() {
        if (parent == null) {
            return ((SearchResponse)response).getBufferSize();
        } else {
            return ((SearchOperation)parent).getBufferSize();
        }
    }
}
