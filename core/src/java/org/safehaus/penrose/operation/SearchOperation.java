package org.safehaus.penrose.operation;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.control.Control;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SearchOperation extends Operation {

    public SearchRequest getSearchRequest();
    public void setSearchRequest(SearchRequest searchRequest);

    public SearchResponse getSearchResponse();
    public void setSearchResponse(SearchResponse searchResponse);

    public DN getDn();
    public void setDn(DN dn);

    public Filter getFilter();
    public void setFilter(Filter filter);

    public int getScope();
    public void setScope(int scope);

    public int getDereference();
    public void setDereference(int dereference);

    public boolean isTypesOnly();
    public void setTypesOnly(boolean typesOnly);

    public Collection<String> getAttributes();
    public void setAttributes(Collection<String> attributes);

    public long getSizeLimit();
    public void setSizeLimit(long sizeLimit);

    public long getTimeLimit();
    public void setTimeLimit(long timeLimit);

    public void setBufferSize(long bufferSize);
    public long getBufferSize();

    public void add(SearchResult result) throws Exception;
    public void add(SearchReference reference) throws Exception;

    public void close() throws Exception;
    public boolean isClosed();

    public long getTotalCount();

    public long getCreateTimestamp();
    public long getCloseTimestamp();
}
