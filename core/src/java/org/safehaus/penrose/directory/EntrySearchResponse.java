package org.safehaus.penrose.directory;

import org.safehaus.penrose.acl.ACLEvaluator;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class EntrySearchResponse extends Pipeline {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Session session;
    SearchRequest request;
    SearchResponse response;
    Entry entry;

    SchemaManager schemaManager;
    ACLEvaluator aclEvaluator;

    Collection<String> requestedAttributes;
    boolean allRegularAttributes;
    boolean allOpAttributes;

    public EntrySearchResponse(
            Session session,
            SearchRequest request,
            SearchResponse response,
            Entry entry
    ) {
        super(response);

        this.session = session;
        this.request = request;
        this.response = response;
        this.entry = entry;

        Partition partition = entry.getPartition();
        this.schemaManager = partition.getSchemaManager();
        this.aclEvaluator = partition.getAclEvaluator();

        requestedAttributes = request.getAttributes();
        allRegularAttributes = requestedAttributes.isEmpty() || requestedAttributes.contains("*");
        allOpAttributes = requestedAttributes.contains("+");
    }

    public void add(SearchResult result) throws Exception {

        try {
            entry.validatePermission(session, result);

        } catch (Exception e) {
            return;
        }

        if (!entry.validateSearchResult(request, result)) return;

        aclEvaluator.filterAttributes(session, result);
        filterAttributes(result);

        super.add(result);
    }

    public void filterAttributes(SearchResult result) throws Exception {

        Attributes attributes = result.getAttributes();
        Collection<String> attributeNames = attributes.getNames();

        if (debug) {
            log.debug("Attribute names: "+attributeNames);
        }

        if (allRegularAttributes && allOpAttributes) {
            if (debug) log.debug("Returning all attributes.");
            return;
        }

        if (allRegularAttributes) {
            if (debug) log.debug("Returning regular attributes.");

            for (String attributeName : attributeNames) {
                if (schemaManager.isOperational(attributeName)) {
                    if (debug) log.debug("Remove operational attribute " + attributeName);
                    attributes.remove(attributeName);
                }
            }

        } else if (allOpAttributes) {
            if (debug) log.debug("Returning operational attributes.");

            for (String attributeName : attributeNames) {
                if (!schemaManager.isOperational(attributeName)) {
                    if (debug) log.debug("Remove regular attribute " + attributeName);
                    attributes.remove(attributeName);
                }
            }

        } else {
            if (debug) log.debug("Returning requested attributes.");
            attributes.retain(requestedAttributes);
        }

        if (debug) log.debug("Returning: "+attributes.getNames());
    }
}
