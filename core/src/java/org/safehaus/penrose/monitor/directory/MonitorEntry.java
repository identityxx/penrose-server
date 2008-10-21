package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntrySearchOperation;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.SearchOperation;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.pipeline.Pipeline;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class MonitorEntry extends Entry {

    protected MBeanServer mbeanServer;

    protected Collection<Entry> children = new ArrayList<Entry>();

    public void init() throws Exception {

        mbeanServer = ManagementFactory.getPlatformMBeanServer();

        DN entryDn = getDn();

        DN partitionsDn = new RDN("cn=Partitions").append(entryDn);

        EntryConfig partitionsEntryConfig = new EntryConfig(partitionsDn);
        partitionsEntryConfig.addObjectClass("monitoredObject");
        partitionsEntryConfig.addAttributesFromRdn();

        PartitionsMonitorEntry partitionsEntry = new PartitionsMonitorEntry();
        partitionsEntry.init(partitionsEntryConfig, entryContext);

        children.add(partitionsEntry);

        DN memoryDn = new RDN("cn=Memory").append(entryDn);

        EntryConfig memoryEntryConfig = new EntryConfig(memoryDn);
        memoryEntryConfig.addObjectClass("monitoredObject");
        memoryEntryConfig.addAttributesFromRdn();

        MemoryMonitorEntry memoryEntry = new MemoryMonitorEntry();
        memoryEntry.init(memoryEntryConfig, entryContext);

        children.add(memoryEntry);

        DN runtimeDn = new RDN("cn=Runtime").append(entryDn);

        EntryConfig runtimeEntryConfig = new EntryConfig(runtimeDn);
        runtimeEntryConfig.addObjectClass("monitoredObject");
        runtimeEntryConfig.addAttributesFromRdn();

        RuntimeMonitorEntry runtimeEntry = new RuntimeMonitorEntry();
        runtimeEntry.init(runtimeEntryConfig, entryContext);

        children.add(runtimeEntry);

        super.init();
    }

    public boolean contains(DN dn) throws Exception {

        DN entryDn = getDn();

        if (entryDn.getSize() == dn.getSize()) {
            return entryDn.matches(dn);

        } else if (entryDn.getSize() < dn.getSize()) {
            return entryDn.endsWith(dn);
        }

        return false;
    }

    public Collection<Entry> findEntries(DN dn) throws Exception {

        if (dn == null) return EMPTY_ENTRIES;

        DN entryDn = getDn();

        if (!dn.endsWith(entryDn)) {
            return EMPTY_ENTRIES;
        }

        Collection<Entry> results = new ArrayList<Entry>();

        if (debug) log.debug("Found entry \""+entryDn+"\".");
        results.add(this);

        return results;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Filter
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateScope(SearchOperation operation) throws Exception {
        // ignore
    }

    public void validateFilter(SearchOperation operation) throws Exception {
        // ignore
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
            log.debug(TextUtil.displayLine("MONITOR SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        EntrySearchOperation op = new EntrySearchOperation(operation, this);

        try {
            validate(op);

            expand(op);

        } finally {
            op.close();
        }
    }

    public void expand(
            SearchOperation operation
    ) throws Exception {

        DN baseDn = operation.getDn();
        DN entryDn = getDn();

        if (baseDn.matches(entryDn)) {
            searchBaseEntry(operation);
            return;
        }
/*
        DN memoryMonitorDn = createMemoryMonitorDN(entryDn);

        if (baseDn.matches(memoryMonitorDn)) {
            searchMemoryMonitor(session, request, response);
            return;
        }

        DN runtimeMonitorDn = createRuntimeMonitorDN(entryDn);

        if (baseDn.matches(runtimeMonitorDn)) {
            searchRuntimeMonitor(session, request, response);
            return;
        }
*/
        if (baseDn.endsWith(entryDn)) {
            log.error("Entry "+baseDn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }
    }

    public void searchBaseEntry(
            SearchOperation operation
    ) throws Exception {

        SearchRequest request = (SearchRequest)operation.getRequest();
        SearchResponse response = (SearchResponse)operation.getResponse();

        DN baseDn = operation.getDn();
        int scope = operation.getScope();

        if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB) {
            SearchResult result = createBaseSearchResult(baseDn);
            operation.add(result);
        }

        if (scope == SearchRequest.SCOPE_ONE || scope == SearchRequest.SCOPE_SUB) {

            SearchOperation op = new SearchOperation(operation) {
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
/*
        DN memoryDn = createMemoryMonitorDN(baseDn);

        if (scope == SearchRequest.SCOPE_ONE || scope == SearchRequest.SCOPE_SUB) {
            SearchResult result = createMemoryMonitorSearchResult(memoryDn);
            response.add(result);
        }

        DN runtimeDn = createRuntimeMonitorDN(baseDn);

        if (scope == SearchRequest.SCOPE_ONE || scope == SearchRequest.SCOPE_SUB) {
            SearchResult result = createRuntimeMonitorSearchResult(runtimeDn);
            response.add(result);
        }
*/
    }
/*
    public void searchMemoryMonitor(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        DN memoryMonitorDn = request.getDn();
        int scope = request.getScope();

        if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB) {
            SearchResult result = createMemoryMonitorSearchResult(memoryMonitorDn);
            response.add(result);
        }
    }

    public void searchRuntimeMonitor(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        DN runtimeMonitorDn = request.getDn();
        int scope = request.getScope();

        if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB) {
            SearchResult result = createRuntimeMonitorSearchResult(runtimeMonitorDn);
            response.add(result);
        }
    }
*/
    public SearchResult createBaseSearchResult(DN baseDn) throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        Attributes attributes = computeAttributes(interpreter);

        SearchResult result = new SearchResult(baseDn, attributes);
        result.setEntryId(getId());

        return result;
    }
/*
    public DN createMemoryMonitorDN(DN monitorDn) throws Exception {
        return new DN("cn=Memory").append(monitorDn);
    }

    public SearchResult createMemoryMonitorSearchResult(DN memoryMonitorDn) throws Exception {

        ObjectName memoryMBean = ObjectName.getInstance("java.lang:type=Memory");
        CompositeDataSupport heapMemoryUsage = (CompositeDataSupport)mbeanServer.getAttribute(memoryMBean, "HeapMemoryUsage");

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "monitoredObject");

        Long committed = (Long)heapMemoryUsage.get("committed");
        attributes.addValue("committed", committed);

        Long init = (Long)heapMemoryUsage.get("init");
        attributes.addValue("init", init);

        Long max = (Long)heapMemoryUsage.get("max");
        attributes.addValue("max", max);

        Long used = (Long)heapMemoryUsage.get("used");
        attributes.addValue("used", used);
        
        SearchResult result = new SearchResult(memoryMonitorDn, attributes);
        result.setEntry(this);

        return result;
    }

    public DN createRuntimeMonitorDN(DN monitorDn) throws Exception {
        return new DN("cn=Runtime").append(monitorDn);
    }

    public SearchResult createRuntimeMonitorSearchResult(DN runtimeMonitorDn) throws Exception {

        Runtime rt = Runtime.getRuntime();

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "monitoredObject");

        Integer availableProcessors = rt.availableProcessors();
        attributes.addValue("availableProcessors", availableProcessors);

        Long freeMemory = rt.freeMemory();
        attributes.addValue("freeMemory", freeMemory);

        Long maxMemory = rt.maxMemory();
        attributes.addValue("maxMemory", maxMemory);

        Long totalMemory = rt.totalMemory();
        attributes.addValue("totalMemory", totalMemory);

        SearchResult result = new SearchResult(runtimeMonitorDn, attributes);
        result.setEntry(this);

        return result;
    }
*/
}