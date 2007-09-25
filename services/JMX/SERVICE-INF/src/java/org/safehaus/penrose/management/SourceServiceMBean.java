package org.safehaus.penrose.management;

import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.source.SourceConfig;

/**
 * @author Endi Sukma Dewata
 */
public interface SourceServiceMBean {

    public Long getCount() throws Exception;

    public void create() throws Exception;
    public void clear() throws Exception;
    public void drop() throws Exception;

    public SourceConfig getSourceConfig() throws Exception;
    
    public SearchResponse search(SearchRequest request, SearchResponse response) throws Exception;
}
