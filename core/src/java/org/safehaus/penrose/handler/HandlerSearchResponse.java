package org.safehaus.penrose.handler;

import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchResponse;
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
public class HandlerSearchResponse extends SearchResponse {

    public Logger log = LoggerFactory.getLogger(getClass());

    SearchResponse parent;

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
            SearchResponse parent,
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

    public void add(Object object) throws Exception {
        Entry entry = (Entry)object;

        boolean debug = log.isDebugEnabled();

        DN dn = entry.getDn();
        EntryMapping entryMapping = entry.getEntryMapping();

        if (!session.isRootUser()) {
            if (debug) log.debug("Checking read permission.");
            
            int rc = aclManager.checkRead(session, partition, entryMapping, dn);
            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Entry \""+entry.getDn()+"\" is not readable.");
                return;
            }
            handlerManager.filterAttributes(session, partition, entry);
        }

        Collection list = (Collection) attributesToRemove.get(entryMapping.getId());
        if (list == null) {
            list = handlerManager.filterAttributes(session, partition, entry, requestedAttributes, allRegularAttributes, allOpAttributes);
            attributesToRemove.put(entryMapping.getId(), list);
        }

        if (debug) log.debug("Removing attributes: "+list);
        handlerManager.removeAttributes(entry, list);

        if (debug) {
            log.debug("Returning "+dn+":");
            entry.getAttributeValues().print();
        }

        parent.add(entry);
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
