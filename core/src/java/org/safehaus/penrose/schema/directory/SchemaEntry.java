package org.safehaus.penrose.schema.directory;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.util.TextUtil;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchemaEntry extends Entry {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Filter
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

     public void validateFilter(Filter filter) throws Exception {
        // ignore
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
            log.debug(TextUtil.displayLine("SCHEMA SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        try {
            validateSearchRequest(session, request, response);

        } catch (Exception e) {
            response.close();
            return;
        }

        response = createSearchResponse(session, request, response);

        try {
            expand(session, request, response);

        } finally {
            response.close();
        }
    }

    public void expand(
            Session session,
            SearchRequest request,
            SearchResponse response
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

        response.add(result);
    }
}
