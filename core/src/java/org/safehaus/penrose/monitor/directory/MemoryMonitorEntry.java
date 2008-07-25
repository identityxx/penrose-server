package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.session.Session;
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
public class MemoryMonitorEntry extends Entry {

    protected MBeanServer mbeanServer;
    protected ObjectName memoryMBean;

    public void init() throws Exception {
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        memoryMBean = ObjectName.getInstance("java.lang:type=Memory");

        super.init();
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
            log.debug(TextUtil.displayLine("MEMORY MONITOR SEARCH", 80));
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
        result.setEntryId(getId());

        response.add(result);
    }
}