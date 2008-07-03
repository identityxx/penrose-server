package org.safehaus.penrose.activeDirectory.directory;

import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.directory.SchemaEntry;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.util.TextUtil;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class ActiveDirectorySchemaEntry extends SchemaEntry {

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
            log.debug(TextUtil.displayLine("ACTIVE DIRECTORY SCHEMA SEARCH", 80));
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

        Interpreter interpreter = partition.newInterpreter();

        DN dn = computeDn(interpreter);
        final Attributes attributes = computeAttributes(interpreter);

        Collection<SourceRef> localSourceRefs = getLocalSourceRefs();
        SourceRef sourceRef = localSourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        SearchRequest newRequest = new SearchRequest();

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                Attributes attr = result.getAttributes();
                Attribute objectClass = attr.get("objectClass");

                if (objectClass.containsValue("attributeSchema")) {
                    AttributeType at = createAttributeType(attr);
                    attributes.addValue("attributeTypes", "( "+at+" )");

                } else if (objectClass.containsValue("classSchema")) {
                    ObjectClass oc = createObjectClass(attr);
                    attributes.addValue("objectClasses", "( "+oc+" )");
                }
            }
        };

        source.search(session, newRequest, newResponse);

        SearchResult result = new SearchResult(dn, attributes);
        result.setEntryId(getId());

        response.add(result);
    }

    public AttributeType createAttributeType(Attributes attributes) {

        AttributeType at = new AttributeType();

        String attributeID = (String)attributes.getValue("attributeID");
        at.setOid(attributeID);

        String lDAPDisplayName = (String)attributes.getValue("lDAPDisplayName");
        at.setName(lDAPDisplayName);

        String adminDescription = (String)attributes.getValue("adminDescription");
        if (adminDescription != null) at.setDescription(adminDescription);

        String attributeSyntax = (String)attributes.getValue("attributeSyntax");
        if (attributeSyntax != null) at.setSyntax(attributeSyntax);

        String isSingleValued = (String)attributes.getValue("isSingleValued");
        if (isSingleValued != null) at.setSingleValued(Boolean.valueOf(isSingleValued));

        return at;
    }

    public ObjectClass createObjectClass(Attributes attributes) {

        ObjectClass oc = new ObjectClass();

        String governsID = (String)attributes.getValue("governsID");
        oc.setOid(governsID);

        String lDAPDisplayName = (String)attributes.getValue("lDAPDisplayName");
        oc.setName(lDAPDisplayName);

        String adminDescription = (String)attributes.getValue("adminDescription");
        if (adminDescription != null) oc.setDescription(adminDescription);

        Collection<Object> mustContain = attributes.getValues("mustContain");
        if (mustContain != null) {
            for (Object object : mustContain) {
                oc.addRequiredAttribute((String)object);
            }
        }

        Collection<Object> systemMustContain = attributes.getValues("systemMustContain");
        if (systemMustContain != null) {
            for (Object object : systemMustContain) {
                oc.addRequiredAttribute((String)object);
            }
        }

        Collection<Object> mayContain = attributes.getValues("mayContain");
        if (mayContain != null) {
            for (Object object : mayContain) {
                oc.addOptionalAttribute((String)object);
            }
        }

        Collection<Object> systemMayContain = attributes.getValues("systemMayContain");
        if (systemMayContain != null) {
            for (Object object : systemMayContain) {
                oc.addOptionalAttribute((String)object);
            }
        }

        return oc;
    }
}
