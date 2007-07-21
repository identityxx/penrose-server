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
    public boolean debug = log.isDebugEnabled();

    SearchResponse<SearchResult> response;

    Session session;
    Partition partition;

    HandlerManager handlerManager;
    ACLManager aclManager;

    Collection<String> requestedAttributes;
    boolean allRegularAttributes;
    boolean allOpAttributes;

    Collection<EntryMapping> entryMappings;
    Map<String,Collection<String>> attributesToRemove = new HashMap<String,Collection<String>>();
    Map<String,Exception> results = new HashMap<String,Exception>();

    public HandlerSearchResponse(
            SearchResponse<SearchResult> parent,
            Session session,
            Partition partition,
            HandlerManager handlerManager,
            ACLManager aclManager,
            Collection<String> requestedAttributes,
            boolean allRegularAttributes,
            boolean allOpAttributes,
            Collection<EntryMapping> entryMappings
    ) {
        this.response = parent;
        this.session = session;
        this.partition = partition;

        this.handlerManager = handlerManager;
        this.aclManager = aclManager;

        this.requestedAttributes = requestedAttributes;
        this.allRegularAttributes = allRegularAttributes;
        this.allOpAttributes = allOpAttributes;

        this.entryMappings = entryMappings;
    }

    public void add(SearchResult searchResult) throws Exception {

        DN dn = searchResult.getDn();
        EntryMapping entryMapping = searchResult.getEntryMapping();
        Attributes attributes = searchResult.getAttributes();

        if (!session.isRootUser()) {
            if (debug) log.debug("Checking read permission.");
            
            int rc = aclManager.checkRead(session, partition, entryMapping, dn);
            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Entry \""+searchResult.getDn()+"\" is not readable.");
                return;
            }
            handlerManager.filterAttributes(session, partition, dn, entryMapping, attributes);
        }

        Collection<String> list = attributesToRemove.get(entryMapping.getId());
        if (list == null) {
            list = handlerManager.filterAttributes(session, partition, searchResult, requestedAttributes, allRegularAttributes, allOpAttributes);
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

        response.add(result);
    }

    public void setResult(EntryMapping entryMapping, LDAPException exception) {
        results.put(entryMapping.getId(), exception);
    }

    public void close() throws Exception {

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

        for (EntryMapping entryMapping : entryMappings) {
            LDAPException ldapException = (LDAPException) results.get(entryMapping.getId());

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
            response.setException(exception);

        } else if (noSuchObject != null && !successes) {
            if (debug) log.debug("Returning: "+noSuchObject.getMessage());
            response.setException(noSuchObject);
        } 

        response.close();
    }
}
