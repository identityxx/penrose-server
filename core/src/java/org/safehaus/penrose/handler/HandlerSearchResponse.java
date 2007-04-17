package org.safehaus.penrose.handler;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.acl.ACLManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.ietf.ldap.LDAPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class HandlerSearchResponse extends SearchResponse<SearchResult> {

    public Logger log = LoggerFactory.getLogger(getClass());

    SearchResponse<SearchResult> parent;

    Session session;
    Partition partition;

    HandlerManager handlerManager;
    SchemaManager schemaManager;
    ACLManager aclManager;

    Set requestedAttributes;
    boolean allRegularAttributes;
    boolean allOpAttributes;

    Collection entryMappings;
    Map attributesToRemove = new HashMap();
    Map results = new HashMap();

    public HandlerSearchResponse(
            SearchResponse<SearchResult> parent,
            Session session,
            Partition partition,
            HandlerManager handlerManager,
            SchemaManager schemaManager,
            ACLManager aclManager,
            Set requestedAttributes,
            boolean allRegularAttributes,
            boolean allOpAttributes,
            Collection entryMappings
    ) {
        this.parent = parent;
        this.session = session;
        this.partition = partition;

        this.handlerManager = handlerManager;
        this.schemaManager  = schemaManager;
        this.aclManager = aclManager;

        this.requestedAttributes = requestedAttributes;
        this.allRegularAttributes = allRegularAttributes;
        this.allOpAttributes = allOpAttributes;

        this.entryMappings = entryMappings;
    }

    public void add(SearchResult object) throws Exception {
        SearchResult entry = (SearchResult)object;

        boolean debug = log.isDebugEnabled();

        DN dn = entry.getDn();
        EntryMapping entryMapping = entry.getEntryMapping();
        Attributes attributes = entry.getAttributes();

        if (!session.isRootUser()) {
            if (debug) log.debug("Checking read permission.");
            
            int rc = aclManager.checkRead(session, partition, entryMapping, dn);
            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Entry \""+entry.getDn()+"\" is not readable.");
                return;
            }
            handlerManager.filterAttributes(session, partition, dn, entryMapping, attributes);
        }

        Collection list = (Collection) attributesToRemove.get(entryMapping.getId());
        if (list == null) {
            list = handlerManager.filterAttributes(session, partition, attributes, requestedAttributes, allRegularAttributes, allOpAttributes);
            attributesToRemove.put(entryMapping.getId(), list);
        }

        if (!list.isEmpty()) {
            if (debug) log.debug("Removing attributes: "+list);
            handlerManager.removeAttributes(attributes, list);
        }

        if (debug) {
            log.debug("Returning "+dn+":");
            attributes.print();
        }

        SearchResult result = new SearchResult(dn, attributes);

        parent.add(result);
    }

    public void setResult(EntryMapping entryMapping, LDAPException exception) {
        results.put(entryMapping.getId(), exception);
    }

    public void close() throws Exception {

        boolean debug = log.isDebugEnabled();

        int count = entryMappings.size() - results.size();

        if (count > 0) {
            if (debug) log.debug("Search thread ended. Waiting for "+count+" more.");
            return;
        } else {
            if (debug) log.debug("All search threads have ended.");
        }

        boolean successes = false;
        LDAPException noSuchObject = null;
        LDAPException exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            LDAPException ldapException = (LDAPException)results.get(entryMapping.getId());

            int rc = ldapException.getResultCode();
            switch (rc) {
                case LDAPException.SUCCESS:
                    successes = true;
                    break;

                case LDAPException.NO_SUCH_OBJECT:
                    noSuchObject = ldapException;
                    break;

                default:
                    exception = ldapException;
                    break;
            }
        }

        if (debug) log.debug("Found successes: "+successes);

        if (exception != null) {
            if (debug) log.debug("Returning: "+exception.getMessage());
            parent.setException(exception);

        } else if (noSuchObject != null && !successes) {
            if (debug) log.debug("Returning: "+noSuchObject.getMessage());
            parent.setException(noSuchObject);
        } 

        parent.close();
    }
}
