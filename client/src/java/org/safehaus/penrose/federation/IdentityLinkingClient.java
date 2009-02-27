package org.safehaus.penrose.federation;

import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.SearchRequest;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class IdentityLinkingClient extends ModuleClient implements IdentityLinkingMBean {

    public IdentityLinkingClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, partitionName, name);
    }

    public Collection<IdentityLinkingResult> search(SearchRequest request) throws Exception {
        return (Collection<IdentityLinkingResult>)invoke(
                "search",
                new Object[] { request },
                new String[] { SearchRequest.class.getName() }
        );
    }

    public Collection<SearchResult> searchLinks(SearchResult sourceEntry) throws Exception {
        return (Collection<SearchResult>)invoke(
                "searchLinks",
                new Object[] { sourceEntry },
                new String[] { SearchResult.class.getName() }
        );
    }

    public void linkEntry(DN sourceDn, DN targetDn) throws Exception {
        invoke(
                "linkEntry",
                new Object[] { sourceDn, targetDn },
                new String[] { DN.class.getName(), DN.class.getName() }
        );
    }

    public void unlinkEntry(DN sourceDn, DN targetDn) throws Exception {
        invoke(
                "unlinkEntry",
                new Object[] { sourceDn, targetDn },
                new String[] { DN.class.getName(), DN.class.getName() }
        );
    }

    public SearchResult importEntry(SearchResult sourceEntry) throws Exception {
        return (SearchResult)invoke(
                "importEntry",
                new Object[] { sourceEntry },
                new String[] { SearchResult.class.getName() }
        );
    }

    public SearchResult importEntry(DN sourceDn, SearchResult targetEntry) throws Exception {
        return (SearchResult)invoke(
                "importEntry",
                new Object[] { sourceDn, targetEntry },
                new String[] { DN.class.getName(), SearchResult.class.getName() }
        );
    }

    public void addEntry(DN targetDn, Attributes targetAttributes) throws Exception {
        invoke(
                "addEntry",
                new Object[] { targetDn, targetAttributes },
                new String[] { DN.class.getName(), Attributes.class.getName() }
        );
    }

    public void deleteEntry(DN sourceDn, DN targetDn) throws Exception {
        invoke(
                "deleteEntry",
                new Object[] { sourceDn, targetDn },
                new String[] { DN.class.getName(), DN.class.getName() }
        );
    }
}
