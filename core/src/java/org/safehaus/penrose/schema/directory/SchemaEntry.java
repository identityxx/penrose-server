package org.safehaus.penrose.schema.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntrySearchOperation;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.util.TextUtil;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchemaEntry extends Entry {

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
            log.debug(TextUtil.displayLine("SCHEMA SEARCH", 80));
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

        DN dn = computeDn(interpreter);
        Attributes attributes = computeAttributes(interpreter);

        SchemaManager schemaManager = partition.getSchemaManager();

        Collection<AttributeType> attributeTypes = schemaManager.getAttributeTypes();
        if (debug) log.debug("Returning "+attributeTypes.size()+" attribute type(s).");

        for (AttributeType attributeType : attributeTypes) {
            attributes.addValue("attributeTypes", "( "+attributeType+" )");
        }

        Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses();
        if (debug) log.debug("Returning "+objectClasses.size()+" object class(es).");

        for (ObjectClass objectClass : objectClasses) {
            attributes.addValue("objectClasses", "( "+objectClass+" )");
        }

        SearchResult result = new SearchResult(dn, attributes);
        result.setEntryId(getId());

        operation.add(result);
    }
}
