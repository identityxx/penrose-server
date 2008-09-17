package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionsMonitorEntry extends Entry {

    protected Collection<Entry> children = new ArrayList<Entry>();

    public void init() throws Exception {

        DN entryDn = getDn();
        DN partitionsDn = new RDN("cn=...").append(entryDn);

        EntryConfig partitionsEntryConfig = new EntryConfig(partitionsDn);
        partitionsEntryConfig.addObjectClass("monitoredObject");

        PartitionMonitorEntry partitionEntry = new PartitionMonitorEntry();
        partitionEntry.init(partitionsEntryConfig, entryContext);

        children.add(partitionEntry);
    }

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
            log.debug(TextUtil.displayLine("PARTITIONS MONITOR SEARCH", 80));
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
            expand(session, request, response);

        } finally {
            response.close();
        }
    }

    public void expand(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        int scope = request.getScope();

        SearchResult result = createBaseSearchResult();
        response.add(result);

        if (scope == SearchRequest.SCOPE_SUB) {

            for (Entry entry : children) {
                log.debug("Searching "+entry.getDn());

                SearchResponse sr = new Pipeline(response) {
                    public void add(SearchResult result) throws Exception {
                        log.debug("Returning "+result.getDn());
                        super.add(result);
                    }
                    public void close() throws Exception {
                        //super.close();
                    }
                };

                entry.search(session, request, sr);
            }
        }
   }

    public SearchResult createBaseSearchResult(
    ) throws Exception {

        DN entryDn = getDn();

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "monitoredObject");
        attributes.addValue("cn", "Partitions");

        SearchResult result = new SearchResult(entryDn, attributes);
        result.setEntryId(getId());

        return result;
    }
}