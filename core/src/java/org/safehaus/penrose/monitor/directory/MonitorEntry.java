package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntrySearchOperation;
import org.safehaus.penrose.directory.EntryContext;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.util.TextUtil;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;

/**
 * @author Endi Sukma Dewata
 */
public class MonitorEntry extends Entry {

    protected MBeanServer mbeanServer;

    public void init() throws Exception {

        mbeanServer = ManagementFactory.getPlatformMBeanServer();

        DN entryDn = getDn();

        DN partitionsDn = new RDN("cn=Partitions").append(entryDn);

        EntryConfig partitionsEntryConfig = new EntryConfig();
        partitionsEntryConfig.setName(getName()+"_partitions");
        partitionsEntryConfig.setDn(partitionsDn);
        partitionsEntryConfig.addObjectClass("monitoredObject");
        partitionsEntryConfig.addAttributesFromRdn();

        EntryContext partitionsEntryContext = new EntryContext();
        partitionsEntryContext.setDirectory(directory);
        partitionsEntryContext.setParent(this);

        PartitionsEntry partitionsEntry = new PartitionsEntry();
        partitionsEntry.init(partitionsEntryConfig, partitionsEntryContext);

        addChild(partitionsEntry);

        DN memoryDn = new RDN("cn=Memory").append(entryDn);

        EntryConfig memoryEntryConfig = new EntryConfig();
        memoryEntryConfig.setName(getName()+"_memory");
        memoryEntryConfig.setDn(memoryDn);
        memoryEntryConfig.addObjectClass("monitoredObject");
        memoryEntryConfig.addAttributesFromRdn();

        EntryContext memoryEntryContext = new EntryContext();
        memoryEntryContext.setDirectory(directory);
        memoryEntryContext.setParent(this);

        MemoryEntry memoryEntry = new MemoryEntry();
        memoryEntry.init(memoryEntryConfig, memoryEntryContext);

        addChild(memoryEntry);

        DN runtimeDn = new RDN("cn=Runtime").append(entryDn);

        EntryConfig runtimeEntryConfig = new EntryConfig();
        runtimeEntryConfig.setName(getName()+"_runtime");
        runtimeEntryConfig.setDn(runtimeDn);
        runtimeEntryConfig.addObjectClass("monitoredObject");
        runtimeEntryConfig.addAttributesFromRdn();

        EntryContext runtimeEntryContext = new EntryContext();
        runtimeEntryContext.setDirectory(directory);
        runtimeEntryContext.setParent(this);

        RuntimeEntry runtimeEntry = new RuntimeEntry();
        runtimeEntry.init(runtimeEntryConfig, runtimeEntryContext);

        addChild(runtimeEntry);
        
        DN sessionsDn = new RDN("cn=Sessions").append(entryDn);

        EntryConfig sessionsEntryConfig = new EntryConfig();
        sessionsEntryConfig.setName(getName()+"_sessions");
        sessionsEntryConfig.setDn(sessionsDn);
        sessionsEntryConfig.addObjectClass("monitoredObject");
        sessionsEntryConfig.addAttributesFromRdn();

        EntryContext sessionsEntryContext = new EntryContext();
        sessionsEntryContext.setDirectory(directory);
        sessionsEntryContext.setParent(this);

        SessionsEntry sessionsEntry = new SessionsEntry();
        sessionsEntry.init(sessionsEntryConfig, sessionsEntryContext);

        addChild(sessionsEntry);

        super.init();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Filter
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean validateScope(SearchOperation operation) throws Exception {
        return true;
    }

    public boolean validateFilter(SearchOperation operation) throws Exception {
        return true;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        final DN baseDn     = operation.getDn();
        final Filter filter = operation.getFilter();
        final int scope     = operation.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("MONITOR SEARCH", 70));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 70));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 70));
            log.debug(TextUtil.displayLine("Filter : "+filter, 70));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        EntrySearchOperation op = new EntrySearchOperation(operation, this);

        try {
            if (!validate(op)) return;

            expand(op);

        } finally {
            op.close();
        }
    }

    public void expand(
            SearchOperation operation
    ) throws Exception {

        DN entryDn = getDn();

        DN baseDn = operation.getDn();
        int scope = operation.getScope();

        int baseLength = baseDn.getLength();
        int entryLength = entryDn.getLength();

        if (baseLength < entryLength && scope == SearchRequest.SCOPE_SUB
                || baseLength == entryLength-1 && scope == SearchRequest.SCOPE_ONE
                || baseDn.matches(entryDn) && (scope == SearchRequest.SCOPE_SUB || scope == SearchRequest.SCOPE_BASE)) {

            SearchResult result = createSearchResult(operation);
            operation.add(result);
        }
    }

    public SearchResult createSearchResult(
            SearchOperation operation
    ) throws Exception {

        DN baseDn = operation.getDn();

        Interpreter interpreter = partition.newInterpreter();

        Attributes attributes = computeAttributes(interpreter);

        SearchResult result = new SearchResult(baseDn, attributes);
        result.setEntryName(getName());

        return result;
    }
}