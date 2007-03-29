package org.safehaus.penrose.adapter.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.DNBuilder;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Source;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SearchRequestBuilder {

    Logger log = LoggerFactory.getLogger(getClass());


    Partition partition;
    EntryMapping entryMapping;

    Collection sources;
    AttributeValues sourceValues;

    Interpreter interpreter;

    SearchRequest request;
    SearchResponse response;

    FilterBuilder filterBuilder;

    Collection requests = new ArrayList();

    public SearchRequestBuilder(
            Partition partition,
            EntryMapping entryMapping,
            Collection sources,
            AttributeValues sourceValues,
            Interpreter interpreter,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        this.partition = partition;
        this.entryMapping = entryMapping;

        this.sources = sources;
        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;

        filterBuilder = new FilterBuilder(
                partition,
                entryMapping,
                sources,
                sourceValues,
                interpreter
        );
    }

    public SearchRequest generate() throws Exception {

        SourceRef sourceRef = (SourceRef)sources.iterator().next();
        generatePrimaryRequest(sourceRef);

        return (SearchRequest)requests.iterator().next();
    }

    public void generatePrimaryRequest(SourceRef sourceRef) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        Source source = sourceRef.getSource();
        if (debug) log.debug("Processing source "+sourceName);

        SearchRequest newRequest = new SearchRequest();

        DNBuilder db = new DNBuilder();
        db.set(source.getParameter(LDAPAdapter.BASE_DN));

        Filter filter = filterBuilder.getFilter();
        if (debug) log.debug("Base filter: "+filter);

        filterBuilder.append(request.getFilter());
        filter = filterBuilder.getFilter();
        if (debug) log.debug("Added search filter: "+filter);

        String scope = source.getParameter(LDAPAdapter.SCOPE);

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
            FieldRef fieldRef = sourceRef.getFieldRef(fieldName);

            if (fieldRef.isPrimaryKey()) {
                RDNBuilder rb = new RDNBuilder();
                rb.set(fieldName, sf.getValue());
                db.prepend(rb.toRdn());

                newRequest.setScope(SearchRequest.SCOPE_BASE);
            }
        }

        newRequest.setDn(db.toDn());

        String defaultFilter = source.getParameter(LDAPAdapter.FILTER);

        if (defaultFilter != null) {
            filter = FilterTool.appendAndFilter(filter, FilterTool.parseFilter(defaultFilter));
            if (debug) log.debug("Added default filter: "+filter);
        }

        newRequest.setFilter(filter);

        long sizeLimit = request.getSizeLimit();
        String s = source.getParameter(SourceConfig.SIZE_LIMIT);
        long maxSizeLimit = s == null ? SourceConfig.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        newRequest.setSizeLimit((sizeLimit > 0 && maxSizeLimit > 0) ? Math.min(sizeLimit, maxSizeLimit) : Math.max(sizeLimit, maxSizeLimit));

        long timeLimit = request.getTimeLimit();
        s = source.getParameter(SourceConfig.TIME_LIMIT);
        long maxTimeLimit = s == null ? SourceConfig.DEFAULT_TIME_LIMIT : Integer.parseInt(s);

        newRequest.setTimeLimit((timeLimit > 0 && maxTimeLimit > 0) ? Math.min(timeLimit, maxTimeLimit) : Math.max(timeLimit, maxTimeLimit));

        requests.add(newRequest);
    }
}
