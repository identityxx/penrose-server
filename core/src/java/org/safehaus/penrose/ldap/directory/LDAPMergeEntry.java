package org.safehaus.penrose.ldap.directory;

import org.safehaus.penrose.directory.DynamicEntry;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.util.TextUtil;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPMergeEntry extends DynamicEntry {

    public final static int USE_FIRST = 0;
    public final static int MERGE_ALL = 1;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN bindDn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("MERGE BIND", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+bindDn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        SearchResult sr = find(bindDn);
        SourceValues sourceValues = sr.getSourceValues();

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        String dn = null;
        SourceRef sourceRef = null;

        for (SourceRef s : getSourceRefs()) {

            final String alias = s.getAlias();
            log.debug("Searching source "+alias+"...");

            Attributes attributes = sourceValues.get(alias);
            if (attributes == null || attributes.isEmpty()) continue;

            dn = (String)attributes.getValue("dn");
            if (dn == null) continue;

            sourceRef = s;

            break;
        }

        if (dn == null) {
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        DN origDn = new DN(dn);
        log.debug("Orig DN: "+origDn);

        DN sourceBaseDn = new DN(sourceRef.getSource().getParameter(LDAPSource.BASE_DN));
        log.debug("Source Base DN: "+sourceBaseDn);

        DN newBindDn = origDn.getPrefix(sourceBaseDn);
        log.debug("New bind DN: "+newBindDn);

        BindRequest newRequest = (BindRequest)request.clone();
        newRequest.setDn(newBindDn);

        Source source = sourceRef.getSource();
        source.bind(session, newRequest, response);
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
            log.debug(TextUtil.displayLine("MERGE SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        response = createSearchResponse(session, request, response);

        try {
            validateScope(request);
            validatePermission(session, request);
            validateFilter(filter);

        } catch (Exception e) {
            response.close();
            return;
        }

        try {
            generateSearchResults(session, request, response);

        } finally {
            response.close();
        }
    }

    public void generateSearchResults(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        DN baseDn = request.getDn();

        final Set<String> keys = new LinkedHashSet<String>();
        final Interpreter interpreter = partition.newInterpreter();

        SearchRequest newRequest = (SearchRequest)request.clone();

        if (getDn().matches(baseDn)) {
            newRequest.setDn(baseDn.getRdn());
            newRequest.setScope(SearchRequest.SCOPE_BASE);

        } else {
            newRequest.setScope(SearchRequest.SCOPE_SUB);
        }

        for (SourceRef sourceRef : getSourceRefs()) {

            final String alias = sourceRef.getAlias();
            log.debug("Searching source "+alias+":");

            SearchResponse newResponse = new Pipeline(response) {
                public void add(SearchResult result) throws Exception {

                    SourceValues sv = new SourceValues();
                    sv.set(alias, result.getAttributes());

                    interpreter.set(sv);

                    DN newDn = computeDn(interpreter);
                    Attributes newAttributes = computeAttributes(interpreter);

                    interpreter.clear();

                    SearchResult newResult = (SearchResult)result.clone();
                    newResult.setDn(newDn);
                    newResult.setAttributes(newAttributes);
                    newResult.setSourceValues(sv);
                    newResult.setEntry(LDAPMergeEntry.this);

                    log.debug("New entry:");
                    newResult.print();

                    String key = newDn.getNormalizedDn();
                    if (keys.contains(key)) return;

                    keys.add(key);
                    super.add(newResult);
                }
            };

            try {
                Source source = sourceRef.getSource();
                source.search(session, newRequest, newResponse);

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
