package org.safehaus.penrose.partition;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.Entry;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class DefaultPartition extends Partition {

    public void search(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        DN baseDn = request.getDn();
        Collection<String> requestedAttributes = request.getAttributes();

        boolean allRegularAttributes = requestedAttributes.isEmpty() || requestedAttributes.contains("*");
        boolean allOpAttributes = requestedAttributes.contains("+");

        if (debug) {
            log.debug("Normalized base DN: "+baseDn);
            log.debug("Normalized attributes: "+requestedAttributes);
        }

        if (baseDn.matches(LDAP.ROOT_DSE_DN) && request.getScope() == SearchRequest.SCOPE_BASE) {
            SearchResult result = createRootDSE();
            Attributes attrs = result.getAttributes();
            if (debug) {
                log.debug("Before: "+result.getDn());
                attrs.print();
            }

            Collection<String> list = filterAttributes(session, result, requestedAttributes, allRegularAttributes, allOpAttributes);
            removeAttributes(attrs, list);

            if (debug) {
                log.debug("After: "+result.getDn());
                attrs.print();
            }

            response.add(result);
            response.close();

            return;

        } else if (baseDn.matches(LDAP.SCHEMA_DN)) {

            SearchResult result = createSchema();
            Attributes attrs = result.getAttributes();
            if (debug) {
                log.debug("Before: "+result.getDn());
                attrs.print();
            }

            Collection<String> list = filterAttributes(session, result, requestedAttributes, allRegularAttributes, allOpAttributes);
            removeAttributes(attrs, list);

            if (debug) {
                log.debug("After: "+result.getDn());
                attrs.print();
            }

            response.add(result);
            response.close();

            return;
        }

        super.search(session, request, response);
    }

    public SearchResult createRootDSE() throws Exception {

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "top");
        attributes.addValue("objectClass", "extensibleObject");
        attributes.addValue("vendorName", Penrose.VENDOR_NAME);
        attributes.addValue("vendorVersion", Penrose.PRODUCT_NAME+" Server "+Penrose.PRODUCT_VERSION);
        attributes.addValue("supportedLDAPVersion", "3");
        attributes.addValue("subschemaSubentry", LDAP.SCHEMA_DN.toString());

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        Partitions partitions = penroseContext.getPartitions();
        for (Partition partition : partitions.getPartitions()) {
            Directory directory = partition.getDirectory();
            for (Entry entry : directory.getRootEntries()) {
                if (entry.getDn().isEmpty()) continue;
                attributes.addValue("namingContexts", entry.getDn().toString());
            }
        }

        return new SearchResult(LDAP.ROOT_DSE_DN, attributes);
    }

    public SearchResult createSchema() throws Exception {

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "top");
        attributes.addValue("objectClass", "subentry");
        attributes.addValue("objectClass", "subschema");
        attributes.addValue("objectClass", "extensibleObject");

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        for (AttributeType attributeType : schemaManager.getAttributeTypes()) {
            attributes.addValue("attributeTypes", "( "+attributeType+" )");
        }

        for (ObjectClass objectClass : schemaManager.getObjectClasses()) {
            attributes.addValue("objectClasses", "( "+objectClass+" )");
        }

        return new SearchResult(LDAP.SCHEMA_DN, attributes);
    }
}
