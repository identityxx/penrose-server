package org.safehaus.penrose.federation.directory;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.mapping.Mapping;
import org.safehaus.penrose.mapping.MappingRule;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchOperation;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.util.TextUtil;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class FederationEntry extends DynamicEntry {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("BIND", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        SearchResult sr = find(dn);
        SourceAttributes sv = sr.getSourceAttributes();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sv);

        Object userPassword = interpreter.get("g.userPassword");

        if (userPassword != null) {
            log.debug("Binding to Global Repository.");

            EntrySource sourceRef = getSource("g");
            Source source = sourceRef.getSource();

            DN globalDn = new DN((String)interpreter.get("g.dn"));

            BindRequest newRequest = (BindRequest)request.clone();
            newRequest.setDn(globalDn);

            source.bind(session, newRequest, response);

            return;
        }

        EntrySource sourceRef = getSource("n");
        Source source = sourceRef.getSource();

        DN nisDn = new DN((String)interpreter.get("n.dn"));

        log.debug("Binding to NIS Repository.");

        BindRequest newRequest = (BindRequest)request.clone();
        newRequest.setDn(nisDn);

        source.bind(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateFilter(SearchOperation operation) throws Exception {
        // ignore
    }

    public Filter createFilter(Attributes attributes) throws Exception {

        Filter filter = null;

        for (Attribute attribute : attributes.getAll()) {
            String name = attribute.getName();
            for (Object value : attribute.getValues()) {
                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }
        }

        return filter;
    }

    public void expand(
            SearchOperation operation
    ) throws Exception {

        Session session = operation.getSession();
        SearchRequest request = (SearchRequest)operation.getRequest();
        SearchResponse response = (SearchResponse)operation.getResponse();

        DN baseDn = operation.getDn();
        int scope = operation.getScope();

        boolean baseSearch = (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB)
                && getDn().matches(baseDn);

        Filter filter = operation.getFilter();

        Collection<String> attributeNames = operation.getAttributes();
        Collection<String> requestedSources = new HashSet<String>();

        Mapping mapping = getMapping();
        if (mapping != null) {

            for (String attributeName : attributeNames) {
                for (MappingRule rule : mapping.getRules(attributeName)) {
                    String variable = rule.getVariable();
                    if (variable == null) continue;

                    int i = variable.indexOf('.');
                    if (i < 0) continue;

                    String sourceName = variable.substring(0, i);
                    requestedSources.add(sourceName);
                }
            }

            if (debug) log.debug("Requested sources: "+requestedSources);
        }

        SourceAttributes sourceAttributes = new SourceAttributes();
        Interpreter interpreter = partition.newInterpreter();

        log.debug("Extracting source attributes from DN:");
        extractSourceAttributes(baseDn, interpreter, sourceAttributes);

        if (debug) {
            log.debug("Source attributes:");
            sourceAttributes.print();
        }

        EntrySource n = getSource("n");
        EntrySource g = getSource("g");

        String gSearch = g.getSearch();

        Collection<DN> dns = new LinkedHashSet<DN>();

        FilterBuilder filterBuilder = new FilterBuilder(this, sourceAttributes, interpreter);

        Filter nFilter = filterBuilder.convert(filter, n);
        nFilter = FilterTool.appendAndFilter(nFilter, createFilter(sourceAttributes.get("n")));

        Filter gFilter = filterBuilder.convert(filter, g);
        gFilter = FilterTool.appendAndFilter(gFilter, createFilter(sourceAttributes.get("g")));

        if (!baseSearch) { // prevent infinite loop

            if (gFilter != null) {
                if (debug) {
                    log.debug("################################################################");
                    log.debug("Search source g with filter "+gFilter);
                }
                SourceAttributes sa = new SourceAttributes();

                try {
                    SearchResponse gResponse = g.search(session, gFilter);

                    while (gResponse.hasNext()) {

                        SearchResult gResult = gResponse.next();
                        sa.set("g", gResult);

                        Collection<Object> seeAlsoValues = sa.getValues("g", "seeAlso");

                        for (Object seeAlso : seeAlsoValues) {

                            try {
                                SearchResult nResult = n.find(session, (String)seeAlso);
                                sa.set("n", nResult);

                                DN dn = createDn(sa);
                                if (debug) log.debug("Found "+dn);

                                dns.add(dn);

                            } catch (Exception e) {
                                // ignore
                            }
                        }
                    }
                } catch (Exception e) {
                    // ignore
                }
            }

            if (nFilter != null) {
                if (debug) {
                    log.debug("################################################################");
                    log.debug("Search source n with filter "+nFilter);
                }
                SourceAttributes sa = new SourceAttributes();

                try {
                    SearchResponse nResponse = n.search(session, nFilter);

                    while (nResponse.hasNext()) {
                        SearchResult nResult = nResponse.next();
                        sa.set("n", nResult);

                        DN dn = createDn(sa);
                        if (debug) log.debug("Found "+dn);

                        dns.add(dn);
                    }
                } catch (Exception e) {
                    // ignore
                }
            }

            if (nFilter != null || gFilter != null) {
                log.debug("################################################################");
                for (DN dn : dns) {
                    if (debug) log.debug("Returning "+dn);
                    SearchResult result = find(session, dn);
                    response.add(result);
                }
                log.debug("################################################################");
                return;
            }
        }

        SourceAttributes sa = new SourceAttributes();

        SearchResponse nResponse = n.search(session, nFilter);

        while (nResponse.hasNext()) {

            SearchResult nResult = nResponse.next();
            sa.set("n", nResult);

            if (EntrySourceConfig.IGNORE.equals(gSearch)) {
                if (debug) log.debug("Source g is ignored.");
                response.add(createSearchResult(sa));
                continue;
            }

            Object dn = sa.getValue("n", "dn");

            SearchResponse gResponse;
            try {
                gResponse = g.search(
                    session,
                    new SimpleFilter("seeAlso", "=", dn)
                );

            } catch (Exception e) {
                if (EntrySourceConfig.REQUIRED.equals(gSearch)) {
                    if (debug) log.debug("Source g is required.");
                } else {
                    if (debug) log.debug("Source g is optional.");
                    response.add(createSearchResult(sa));
                }
                continue;
            }

            if (!gResponse.hasNext()) {
                if (EntrySourceConfig.REQUIRED.equals(gSearch)) {
                    if (debug) log.debug("Source g is required.");
                } else {
                    if (debug) log.debug("Source g is optional.");
                    response.add(createSearchResult(sa));
                }
                continue;
            }

            do {
                SearchResult gResult = gResponse.next();
                sa.set("g", gResult);

                response.add(createSearchResult(sa));

            } while (gResponse.hasNext());

            sa.remove("g");
        }
    }

    public Date toDate(long accountExpires) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(1601, 0, 1, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        accountExpires = accountExpires / 10000 + calendar.getTime().getTime();
        return new Date(accountExpires);
    }
}
