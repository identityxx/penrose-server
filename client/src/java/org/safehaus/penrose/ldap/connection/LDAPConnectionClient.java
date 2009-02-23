package org.safehaus.penrose.ldap.connection;

import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.connection.ConnectionClient;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.schema.Schema;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPConnectionClient extends ConnectionClient implements LDAPConnectionServiceMBean {

    public LDAPConnectionClient(PenroseClient client, String partitionName, String connectionName) throws Exception {
        super(client, partitionName, connectionName);
    }

    public SearchResult find(String dn) throws Exception {
        return (SearchResult)invoke(
                "find",
                new Object[] { dn },
                new String[] { String.class.getName() }
        );
    }

    public SearchResult find(DN dn) throws Exception {
        return (SearchResult)invoke(
                "find", 
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    public SearchResponse search(SearchRequest request, SearchResponse response) throws Exception {
        SearchResponse newResponse = (SearchResponse)invoke(
                "search",
                new Object[] { request, response },
                new String[] { SearchRequest.class.getName(), SearchResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    public Schema getSchema() throws Exception {
        return (Schema)getAttribute("Schema");
    }
}