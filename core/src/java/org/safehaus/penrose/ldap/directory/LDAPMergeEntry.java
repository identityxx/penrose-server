package org.safehaus.penrose.ldap.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPMergeEntry extends Entry {

    public final static int USE_FIRST = 0;
    public final static int MERGE_ALL = 1;

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("MERGE BIND", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Entry : "+getDn(), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("DN    : "+dn, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        SearchResult sr = find(dn);

        SourceValues sourceValues = sr.getSourceValues();
        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        String bindDn = null;
        SourceRef sourceRef = null;

        for (SourceRef s : getSourceRefs()) {

            final String alias = s.getAlias();
            log.debug("Searching source "+alias+"...");

            Attributes attributes = sourceValues.get(alias);
            if (attributes == null || attributes.isEmpty()) continue;

            bindDn = (String)attributes.getValue("dn");
            if (bindDn == null) continue;

            sourceRef = s;

            log.debug("DN: "+bindDn);
            break;
        }

        if (bindDn == null) {
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        BindRequest newRequest = (BindRequest)request.clone();
        newRequest.setDn(bindDn);

        sourceRef.bind(session, newRequest, response);
    }

    public void searchEntry(
            final Session session,
            final Entry base,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        final DN baseDn     = request.getDn();
        final Filter filter = request.getFilter();
        final int scope     = request.getScope();

        if (debug) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("MERGE SEARCH", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Entry  : "+getDn(), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Base   : "+baseDn, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Filter : "+filter, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        final FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();

        if (!filterEvaluator.eval(this, filter)) {
            if (debug) log.debug("Entry \""+getDn()+"\" doesn't match search filter.");
            return;
        }

        final Set<String> results = new LinkedHashSet<String>();
        final Interpreter interpreter = partition.newInterpreter();

        SearchRequest newRequest = (SearchRequest)request.clone();

        if (base == this) {
            newRequest.setDn(baseDn.getRdn());
            newRequest.setScope(SearchRequest.SCOPE_BASE);

        } else {
            newRequest.setScope(SearchRequest.SCOPE_SUB);
        }

        for (SourceRef sourceRef : getSourceRefs()) {

            final String alias = sourceRef.getAlias();
            log.debug("Searching source "+alias+":");

            SearchResponse newResponse = new SearchResponse() {
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

                    if (!filterEvaluator.eval(newResult, filter)) {
                        if (debug) log.debug("Entry \""+newResult.getDn()+"\" doesn't match search filter.");
                        return;
                    }

                    String key = newDn.getNormalizedDn();
                    if (!results.contains(key)) {
                        response.add(newResult);
                        results.add(key);
                    }
                }
            };

            try {
                sourceRef.search(session, newRequest, newResponse);
                
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }
}
