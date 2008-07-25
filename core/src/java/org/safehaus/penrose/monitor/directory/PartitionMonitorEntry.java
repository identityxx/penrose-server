package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionMonitorEntry extends Entry {

    public SearchResponse createSearchResponse(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) {
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        final DN baseDn     = request.getDn();
        final Filter filter = request.getFilter();
        final int scope     = request.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("PARTITION MONITOR SEARCH", 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        try {
            validateSearchRequest(session, request, response);

        } catch (Exception e) {
            response.close();
            return;
        }

        response = createSearchResponse(session, request, response);

        try {
            executeSearch(session, request, response);

        } finally {
            response.close();
        }
    }

    public void executeSearch(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        PartitionManager partitionManager = partition.getPartitionContext().getPenroseContext().getPartitionManager();
        for (Partition partition : partitionManager.getPartitions()) {
            SearchResult result = createBaseSearchResult(partition);
            response.add(result);
        }
    }

    public SearchResult createBaseSearchResult(
            Partition partition
    ) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        rb.set("cn", partition.getName());
        RDN rdn = rb.toRdn();

        DN entryDn = rdn.append(getParentDn());

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "monitoredObject");
        attributes.addValue("cn", partition.getName());

        SearchResult result = new SearchResult(entryDn, attributes);
        result.setEntryId(getId());

        return result;
    }
}