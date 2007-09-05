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

    public void createCache() throws Exception;
    public void loadCache() throws Exception;
    public void cleanCache() throws Exception;
    public void dropCache() throws Exception;

    public SourceConfig getSourceConfig() throws Exception;
    
    public SearchResponse search(SearchRequest request, SearchResponse response) throws Exception;
}
