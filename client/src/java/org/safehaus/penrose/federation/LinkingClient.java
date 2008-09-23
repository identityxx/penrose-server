package org.safehaus.penrose.federation;

import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.SearchRequest;

import javax.management.MBeanException;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class LinkingClient extends ModuleClient implements LinkingMBean {

    public LinkingClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, partitionName, name);
    }

    public Collection<LinkingData> search(SearchRequest request) throws Exception {
        try {
            return (Collection<LinkingData>)invoke(
                    "search",
                    new Object[] { request },
                    new String[] { SearchRequest.class.getName() }
            );
        } catch (MBeanException e) {
            throw (Exception)e.getCause();
        }
    }

    public Collection<SearchResult> searchLinks(SearchResult sourceEntry) throws Exception {
        try {
            return (Collection<SearchResult>)invoke(
                    "searchLinks",
                    new Object[] { sourceEntry },
                    new String[] { SearchResult.class.getName() }
            );
        } catch (MBeanException e) {
            throw (Exception)e.getCause();
        }
    }

    public void linkEntry(DN sourceDn, DN targetDn) throws Exception {
        try {
            invoke(
                    "linkEntry",
                    new Object[] { sourceDn, targetDn },
                    new String[] { DN.class.getName(), DN.class.getName() }
            );
        } catch (MBeanException e) {
            throw (Exception)e.getCause();
        }
    }

    public void unlinkEntry(DN sourceDn, DN targetDn) throws Exception {
        try {
            invoke(
                    "unlinkEntry",
                    new Object[] { sourceDn, targetDn },
                    new String[] { DN.class.getName(), DN.class.getName() }
            );
        } catch (MBeanException e) {
            throw (Exception)e.getCause();
        }
    }

    public SearchResult importEntry(SearchResult sourceEntry) throws Exception {
        try {
            return (SearchResult)invoke(
                    "importEntry",
                    new Object[] { sourceEntry },
                    new String[] { SearchResult.class.getName() }
            );
        } catch (MBeanException e) {
            throw (Exception)e.getCause();
        }
    }

    public SearchResult importEntry(DN sourceDn, SearchResult targetEntry) throws Exception {
        try {
            return (SearchResult)invoke(
                    "importEntry",
                    new Object[] { sourceDn, targetEntry },
                    new String[] { DN.class.getName(), SearchResult.class.getName() }
            );
        } catch (MBeanException e) {
            throw (Exception)e.getCause();
        }
    }

    public void addEntry(DN targetDn, Attributes targetAttributes) throws Exception {
        try {
            invoke(
                    "addEntry",
                    new Object[] { targetDn, targetAttributes },
                    new String[] { DN.class.getName(), Attributes.class.getName() }
            );
        } catch (MBeanException e) {
            throw (Exception)e.getCause();
        }
    }

    public void deleteEntry(DN targetDn) throws Exception {
        try {
            invoke(
                    "deleteEntry",
                    new Object[] { targetDn },
                    new String[] { DN.class.getName() }
            );
        } catch (MBeanException e) {
            throw (Exception)e.getCause();
        }
    }
}
