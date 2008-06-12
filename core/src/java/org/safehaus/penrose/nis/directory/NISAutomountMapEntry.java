package org.safehaus.penrose.nis.directory;

import org.safehaus.penrose.directory.AttributeMapping;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.directory.DynamicEntry;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.nis.source.NISSource;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.util.TextUtil;

/**
 * @author Endi S. Dewata
 */
public class NISAutomountMapEntry extends DynamicEntry {

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
            log.debug(TextUtil.displayLine("AUTOMOUNT MAP SEARCH", 80));
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

        if (debug) log.debug("Searching entry "+getDn());

        final Interpreter interpreter = partition.newInterpreter();
        final SourceRef primarySourceRef = getSourceRef(0);

        SearchResponse newResponse = new Pipeline(response) {
            public void add(SearchResult primaryResult) throws Exception {

                SourceValues sv = new SourceValues();
                sv.set(primarySourceRef.getAlias(), primaryResult.getAttributes());

                interpreter.set(sv);

                DN dn = computeDn(interpreter);
                Attributes attributes = computeAttributes(interpreter);

                interpreter.clear();

                SearchResult newResult = new SearchResult();
                newResult.setDn(dn);
                newResult.setAttributes(attributes);
                newResult.setEntry(NISAutomountMapEntry.this);

                super.add(newResult);
            }
        };

        NISSource primarySource = (NISSource)primarySourceRef.getSource();

        DN dn = request.getDn();
        RDN rdn = dn.getRdn();

        if (getDn().matches(baseDn)) {

            String automountMapName = (String)rdn.get("nisMapName");

            primarySource.searchAutomountMaps(
                    session,
                    automountMapName,
                    newResponse
            );

        } else if (getParentDn().matches(baseDn)) {

            primarySource.searchAutomountMaps(
                    session,
                    null,
                    newResponse
            );
        }
    }

    public DN computeDn(
            Interpreter interpreter
    ) throws Exception {

        DNBuilder db = new DNBuilder();
        RDNBuilder rb = new RDNBuilder();

        AttributeMapping nisMapName = getAttributeMapping("nisMapName");
        rb.set("nisMapName", interpreter.eval(nisMapName));
        db.append(rb.toRdn());

        Entry parent = getParent();
        db.append(parent.getDn());

        return db.toDn();
    }
}
