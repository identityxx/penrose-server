package org.safehaus.penrose.directory;

import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.interpreter.Interpreter;

/**
 * @author Endi Sukma Dewata
 */
public class RootEntry extends Entry {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Scope
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean validateScope(SearchOperation operation) throws Exception {

        int scope = operation.getScope();

        if (scope != SearchRequest.SCOPE_BASE) {
            log.debug("Entry out of scope.");
            //throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
            return false;
        }

        return true;
    }

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

        final DN baseDn     = operation.getDn();
        final Filter filter = operation.getFilter();
        final int scope     = operation.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("ROOT DSE SEARCH", 80));
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
            SearchOperation operation
    ) throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        DN dn = new DN();
        Attributes attributes = computeAttributes(interpreter);

        SearchResult result = new SearchResult(dn, attributes);
        result.setEntryName(getName());

        operation.add(result);
    }
}