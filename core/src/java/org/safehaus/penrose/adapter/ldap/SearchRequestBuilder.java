package org.safehaus.penrose.adapter.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.DNBuilder;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.adapter.jdbc.*;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SearchRequestBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    LDAPAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    SearchRequest request;
    SearchResponse response;

    FilterBuilder filterBuilder;

    Collection requests = new ArrayList();

    public SearchRequestBuilder(
            LDAPAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        this.adapter = adapter;

        this.partition = partition;
        this.entryMapping = entryMapping;

        this.sourceMappings = sourceMappings;
        primarySourceMapping = (SourceMapping)sourceMappings.iterator().next();

        this.sourceValues = sourceValues;

        this.request = request;
        this.response = response;

        PenroseContext penroseContext = adapter.getPenroseContext();
        interpreter = penroseContext.getInterpreterManager().newInstance();

        filterBuilder = new FilterBuilder(
                partition,
                entryMapping,
                sourceMappings,
                sourceValues,
                interpreter
        );
    }

    public SearchRequest generate() throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        generatePrimaryRequest(sourceMapping);

        return (SearchRequest)requests.iterator().next();
    }

    public void generatePrimaryRequest(SourceMapping sourceMapping) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        SearchRequest newRequest = new SearchRequest();

        DNBuilder db = new DNBuilder();
        db.set(sourceConfig.getParameter(LDAPAdapter.BASE_DN));

        Filter filter = filterBuilder.getFilter();
        if (debug) log.debug("Base filter: "+filter);

        filterBuilder.append(request.getFilter());
        filter = filterBuilder.getFilter();
        if (debug) log.debug("Added search filter: "+filter);

        String scope = sourceConfig.getParameter(LDAPAdapter.SCOPE);

        if ("OBJECT".equals(scope)) {
            newRequest.setScope(SearchRequest.SCOPE_BASE);

        } else if ("ONELEVEL".equals(scope)) {
            newRequest.setScope(SearchRequest.SCOPE_ONE);

        } else if ("SUBTREE".equals(scope)) {
            newRequest.setScope(SearchRequest.SCOPE_SUB);
        }

        if (filter != null && filter instanceof SimpleFilter) {
            SimpleFilter sf = (SimpleFilter)filter;

            String fieldName = sf.getAttribute();
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

            if (fieldConfig.isPrimaryKey()) {
                RDNBuilder rb = new RDNBuilder();
                rb.set(fieldName, sf.getValue());
                db.prepend(rb.toRdn());

                newRequest.setScope(SearchRequest.SCOPE_BASE);
            }
        }

        newRequest.setDn(db.toDn());

        String defaultFilter = sourceConfig.getParameter(LDAPAdapter.FILTER);

        if (defaultFilter != null) {
            filter = FilterTool.appendAndFilter(filter, FilterTool.parseFilter(defaultFilter));
            if (debug) log.debug("Added default filter: "+filter);
        }

        newRequest.setFilter(filter);

        long sizeLimit = request.getSizeLimit();
        String s = sourceConfig.getParameter(SourceConfig.SIZE_LIMIT);
        long maxSizeLimit = s == null ? SourceConfig.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        newRequest.setSizeLimit((sizeLimit > 0 && maxSizeLimit > 0) ? Math.min(sizeLimit, maxSizeLimit) : Math.max(sizeLimit, maxSizeLimit));

        long timeLimit = request.getTimeLimit();
        s = sourceConfig.getParameter(SourceConfig.TIME_LIMIT);
        long maxTimeLimit = s == null ? SourceConfig.DEFAULT_TIME_LIMIT : Integer.parseInt(s);

        newRequest.setTimeLimit((timeLimit > 0 && maxTimeLimit > 0) ? Math.min(timeLimit, maxTimeLimit) : Math.max(timeLimit, maxTimeLimit));

        requests.add(newRequest);
    }
}
