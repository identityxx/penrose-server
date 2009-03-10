package org.safehaus.penrose.monitor.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntrySearchOperation;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.Session;

/**
 * @author Endi Sukma Dewata
 */
public class SessionEntry extends Entry {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Filter
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean validateFilter(SearchOperation operation) throws Exception {
        return true;
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
            log.debug(TextUtil.displayLine("SESSION ENTRY SEARCH", 70));
            log.debug(TextUtil.displayLine("Filter : "+filter, 70));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 70));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 70));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 70));
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

        DN entryDn = getDn();

        DN baseDn = operation.getDn();
        int scope = operation.getScope();

        int baseLength = baseDn.getLength();
        int entryLength = entryDn.getLength();

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();

        if (baseLength < entryLength && scope == SearchRequest.SCOPE_SUB
                || baseLength == entryLength-1 && scope == SearchRequest.SCOPE_ONE) {

            for (String sessionName : sessionManager.getSessionNames()) {
                Session session = sessionManager.getSession(sessionName);
                SearchResult result = createSearchResult(operation, session);
                operation.add(result);
            }

        } else if (baseDn.matches(entryDn) && (scope == SearchRequest.SCOPE_SUB || scope == SearchRequest.SCOPE_BASE)) {

            RDN rdn = baseDn.getRdn();
            String sessionName = (String)rdn.getValue();

            Session session = sessionManager.getSession(sessionName);
            if (session == null) throw LDAP.createException(LDAP.NO_SUCH_OBJECT);

            SearchResult result = createSearchResult(operation, session);
            operation.add(result);
        }
    }

    public SearchResult createSearchResult(
            SearchOperation operation,
            Session session
    ) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        rb.set("cn", session.getSessionName());
        RDN rdn = rb.toRdn();

        DN entryDn = rdn.append(getParentDn());

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "monitoredObject");
        attributes.addValue("cn", session.getSessionName());
        attributes.addValue("bindDn", session.getBindDn());

        SearchResult result = new SearchResult(entryDn, attributes);
        result.setEntryName(getName());

        return result;
    }
}