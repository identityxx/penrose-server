package org.safehaus.penrose.ldap.directory;

import org.safehaus.penrose.directory.DynamicEntry;
import org.safehaus.penrose.directory.EntrySource;
import org.safehaus.penrose.directory.EntrySearchOperation;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.Penrose;

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

        boolean debug = log.isDebugEnabled();

        DN bindDn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("MERGE BIND", 70));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 70));
            log.debug(TextUtil.displayLine("DN    : "+bindDn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        SearchResult sr = find(bindDn);
        SourceAttributes sourceValues = sr.getSourceAttributes();

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        String dn = null;
        EntrySource sourceRef = null;

        for (EntrySource s : getSources()) {

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

        DN sourceBaseDn = new DN(sourceRef.getSource().getParameter(LDAP.BASE_DN));
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
            SearchOperation operation
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        final DN baseDn     = operation.getDn();
        final Filter filter = operation.getFilter();
        final int scope     = operation.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("MERGE SEARCH", 70));
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

        Session session = operation.getSession();
        SearchRequest request = operation.getSearchRequest();
        SearchResponse response = operation.getSearchResponse();

        DN baseDn = operation.getDn();

        final Set<String> keys = new LinkedHashSet<String>();
        final Interpreter interpreter = partition.newInterpreter();

        SearchRequest newRequest = (SearchRequest)request.clone();

        if (getDn().matches(baseDn)) {
            newRequest.setDn(baseDn.getRdn());
            newRequest.setScope(SearchRequest.SCOPE_BASE);

        } else {
            newRequest.setScope(SearchRequest.SCOPE_SUB);
        }

        for (EntrySource sourceRef : getSources()) {

            final String alias = sourceRef.getAlias();
            log.debug("Searching source "+alias+":");

            SearchResponse newResponse = new Pipeline(response) {
                public void add(SearchResult result) throws Exception {

                    SourceAttributes sv = new SourceAttributes();
                    sv.set(alias, result.getAttributes());

                    interpreter.set(sv);

                    DN newDn = computeDn(interpreter);
                    Attributes newAttributes = computeAttributes(interpreter);

                    interpreter.clear();

                    SearchResult newResult = (SearchResult)result.clone();
                    newResult.setDn(newDn);
                    newResult.setAttributes(newAttributes);
                    newResult.setSourceAttributes(sv);
                    newResult.setEntryName(getName());

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
                Penrose.errorLog.error(e.getMessage(), e);
            }
        }
    }
}
