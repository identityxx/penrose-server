package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.operation.PipelineSearchOperation;
import org.safehaus.penrose.util.TextUtil;

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            SearchOperation operation
    ) throws Exception {

        final DN baseDn     = operation.getDn();
        final Filter filter = operation.getFilter();
        final int scope     = operation.getScope();

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
            validate(operation);

            expand(operation);

        } finally {
            operation.close();
        }
    }

    public void expand(
            SearchOperation operation
    ) throws Exception {

        SearchRequest request = (SearchRequest)operation.getRequest();
        SearchResponse response = (SearchResponse)operation.getResponse();
        int scope = operation.getScope();

        SearchResult result = createBaseSearchResult();
        operation.add(result);

        if (scope == SearchRequest.SCOPE_SUB) {

            SearchOperation op = new PipelineSearchOperation(operation) {
                public void add(SearchResult result) throws Exception {
                    log.debug("Returning "+result.getDn());
                    super.add(result);
                }
                public void close() throws Exception {
                    //super.close();
                }
            };

            for (Entry entry : children) {
                log.debug("Searching "+entry.getDn());

                entry.search(op);
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