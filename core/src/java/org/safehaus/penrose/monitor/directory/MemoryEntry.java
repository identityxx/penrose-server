package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.TextUtil;

import javax.management.ObjectName;
import javax.management.MBeanServer;
import javax.management.openmbean.CompositeDataSupport;
import java.lang.management.ManagementFactory;

/**
 * @author Endi Sukma Dewata
 */
public class MemoryEntry extends Entry {

    protected MBeanServer mbeanServer;
    protected ObjectName memoryMBean;

    public void init() throws Exception {
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        memoryMBean = ObjectName.getInstance("java.lang:type=Memory");

        super.init();
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
            log.debug(TextUtil.displayLine("MEMORY ENTRY SEARCH", 70));
            log.debug(TextUtil.displayLine("Filter : "+filter, 70));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 70));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 70));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        try {
            if (!validate(operation)) return;

            expand(operation);

        } finally {
            operation.close();
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

        CompositeDataSupport heapMemoryUsage = (CompositeDataSupport)mbeanServer.getAttribute(memoryMBean, "HeapMemoryUsage");

        DN entryDn = getDn();

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

        SearchResult result = new SearchResult(entryDn, attributes);
        result.setEntryName(getName());

        return result;
    }
}