package org.safehaus.penrose.nis.directory;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.util.TextUtil;
import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class NISEntry extends DynamicEntry {

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
            log.debug(TextUtil.displayLine("NIS SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
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
            final SearchOperation operation
    ) throws Exception {

        final Session session = operation.getSession();
        SearchResponse response = operation.getSearchResponse();

        DN baseDn = operation.getDn();

        if (debug) log.debug("Searching entry "+baseDn);

        final Interpreter interpreter = partition.newInterpreter();
        final EntrySource primarySourceRef = getSource(0);

        SearchRequest newRequest = createSearchRequest(session, operation, interpreter);

        SearchResponse newResponse = new Pipeline(response) {
            public void add(SearchResult primaryResult) throws Exception {

                SourceAttributes sv = new SourceAttributes();
                sv.set(primarySourceRef.getAlias(), primaryResult.getAttributes());

                interpreter.set(sv);

                for (int i=1; i< getSourceCount(); i++) {
                    try {
                        EntrySource source = getSource(i);

                        SearchResult secondaryResult = find(session, source, interpreter);
                        if (secondaryResult == null) continue;

                        sv.set(source.getAlias(), secondaryResult.getAttributes());
                        interpreter.set(sv);

                    } catch (LDAPException e) {
                        log.debug(e.getMessage());
                        
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                DN dn = computeDn(interpreter);
                Attributes attributes = computeAttributes(interpreter);

                interpreter.clear();

                SearchResult newResult = new SearchResult();
                newResult.setDn(dn);
                newResult.setAttributes(attributes);
                newResult.setEntryName(getName());

                super.add(newResult);
            }
        };

        try {
            Source source = primarySourceRef.getSource();
            source.search(session, newRequest, newResponse);
            
        } catch (Exception e) {
            response.setException(e);
        }
    }

    public SearchRequest createSearchRequest(
            Session session,
            SearchOperation operation,
            Interpreter interpreter
    ) throws Exception {

        SearchRequest request = operation.getSearchRequest();
        SearchRequest newRequest = (SearchRequest)request.clone();

        DN baseDn = operation.getDn();
        Filter filter = operation.getFilter();
        int scope = operation.getScope();

        final EntrySource primarySourceRef = getSource(0);

        DN primaryBaseDn = null;
        Filter primaryFilter = null;

        if (getDn().matches(baseDn) && (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB)) {
            primaryBaseDn = createPrimaryBaseDn(session, primarySourceRef, baseDn, interpreter);

            if (primaryBaseDn == null) {
                interpreter.set(baseDn.getRdn());

                for (EntryField fieldRef : primarySourceRef.getFields()) {

                    Object value = interpreter.eval(fieldRef);
                    if (value == null) continue;

                    String name = fieldRef.getName();
                    SimpleFilter sf = new SimpleFilter(name, "=", value);
                    primaryFilter = FilterTool.appendAndFilter(sf, primaryFilter);
                }

                interpreter.clear();
            }
        }

        newRequest.setDn(primaryBaseDn);

        Filter newFilter = createPrimaryFilter(session, primarySourceRef, filter, interpreter);
        primaryFilter = FilterTool.appendOrFilter(newFilter, primaryFilter);

        newRequest.setFilter(primaryFilter);

        return newRequest;
    }

    public DN createPrimaryBaseDn(
            Session session,
            EntrySource primarySourceRef,
            DN baseDn,
            Interpreter interpreter
    ) throws Exception {

        try {
            interpreter.set(baseDn.getRdn());

            RDNBuilder rb = new RDNBuilder();

            for (EntryField fieldRef : primarySourceRef.getPrimaryKeyFields()) {

                Object value = interpreter.eval(fieldRef);
                if (value == null) return null;

                String fieldName = fieldRef.getName();
                rb.set(fieldName, value);
            }

            return new DN(rb.toRdn());

        } finally {
            interpreter.clear();
        }
    }

    public Filter createPrimaryFilter(
            Session session,
            EntrySource primarySourceRef,
            Filter filter,
            Interpreter interpreter
    ) throws Exception {

        SourceAttributes sa = new SourceAttributes();
        FilterBuilder filterBuilder = new FilterBuilder(this, sa, interpreter);
        Filter primaryFilter = filterBuilder.convert(filter, primarySourceRef);

        for (int i= getSourceCount()-1; i>0; i--) {
            try {
                EntrySource sourceRef = getSource(i);

                Filter sourceFilter = filterBuilder.convert(filter, sourceRef);
                if (sourceFilter == null) continue;

                SearchRequest newRequest = new SearchRequest();
                newRequest.setDn((DN)null);
                newRequest.setFilter(sourceFilter);

                SearchResponse newResponse = new SearchResponse();

                Source source = sourceRef.getSource();
                source.search(session, newRequest, newResponse);

                Filter newFilter = createFilter(primarySourceRef, sourceRef, newResponse.getResults());
                primaryFilter = FilterTool.appendOrFilter(newFilter, primaryFilter);

            } catch (Exception e) {
                log.debug(e.getMessage(), e);
            }
        }

        return primaryFilter;
    }

    public Filter createFilter(
            EntrySource primarySourceRef,
            EntrySource sourceRef,
            Collection<SearchResult> results
    ) throws Exception {

        String primaryAlias = primarySourceRef.getAlias();

        Filter filter = null;

        for (SearchResult result : results) {
            Attributes attributes = result.getAttributes();

            Filter af = null;
            for (EntryField fieldRef : sourceRef.getFields()) {
                String variable = fieldRef.getVariable();
                if (variable == null) continue;
                if (!variable.startsWith(primaryAlias+".")) continue;

                Object value = attributes.getValue(fieldRef.getName());
                if (value == null) continue;

                String name = variable.substring(primaryAlias.length()+1);
                SimpleFilter sf = new SimpleFilter(name, "=", value);
                af = FilterTool.appendAndFilter(sf, af);
            }

            filter = FilterTool.appendOrFilter(af, filter);
        }

        return filter;
    }

    public SearchResult find(
            final Session session,
            final EntrySource sourceRef,
            final Interpreter interpreter
    ) throws Exception {

        if (debug) log.debug("Searching source "+sourceRef.getAlias());

        RDNBuilder rb = new RDNBuilder();

        for (EntryField fieldRef : sourceRef.getFields()) {

            Object value = interpreter.eval(fieldRef);
            if (value == null) continue;

            rb.set(fieldRef.getName(), value);
        }

        if (rb.isEmpty()) return null;

        Source source = sourceRef.getSource();
        return source.find(session, rb.toRdn());
    }
}
