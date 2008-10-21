package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.session.SearchOperation;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.TextUtil;

import javax.management.ObjectName;
import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;

/**
 * @author Endi Sukma Dewata
 */
public class RuntimeMonitorEntry extends Entry {

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

        final DN baseDn     = operation.getDn();
        final Filter filter = operation.getFilter();
        final int scope     = operation.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("RUNTIME MONITOR SEARCH", 80));
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

        Runtime rt = Runtime.getRuntime();

        DN entryDn = getDn();

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

        SearchResult result = new SearchResult(entryDn, attributes);
        result.setEntryId(getId());

        operation.add(result);
    }
}