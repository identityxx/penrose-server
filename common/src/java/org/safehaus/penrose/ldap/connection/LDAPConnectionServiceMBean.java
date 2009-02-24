package org.safehaus.penrose.ldap.connection;

import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.schema.Schema;

/**
 * @author Endi Sukma Dewata
 */
public interface LDAPConnectionServiceMBean {

    public SearchResult find(String dn) throws Exception;
    public SearchResult find(DN dn) throws Exception;

    public SearchResponse search(SearchRequest request, SearchResponse response) throws Exception;

    public Schema getSchema() throws Exception;
}