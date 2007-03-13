package org.safehaus.penrose.handler;

import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.session.PenroseSession;
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
public class SearchPipeline extends Pipeline {

    public Logger log = LoggerFactory.getLogger(getClass());

    PenroseSession session;
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

    public SearchPipeline(
            Results parent,
            PenroseSession session,
            Partition partition,
            HandlerManager handlerManager,
            SchemaManager schemaManager,
            ACLManager aclManager,
            Set requestedAttributes,
            boolean allRegularAttributes,
            boolean allOpAttributes,
            Collection entryMappings
    ) {
        super(parent);

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

    // Perform ACL evaluation, convert Entry into SearchResult.
    public void add(Object object) throws Exception {
        Entry entry = (Entry)object;

        boolean debug = log.isDebugEnabled();

        DN dn = entry.getDn();
        if (debug) log.debug("Returning "+dn);
        
        EntryMapping entryMapping = entry.getEntryMapping();

        if (!session.isRootUser()) {
            if (debug) log.debug("Checking read permission on "+dn);
            int rc = aclManager.checkRead(session, partition, entryMapping, dn);
            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Entry \""+entry.getDn()+"\" is not readable.");
                return;
            }
            handlerManager.filterAttributes(session, partition, entry);
        }

        if (debug) {
            log.debug("Before: "+entry.getDn());
            entry.getAttributeValues().print();
        }

        Collection list = (Collection) attributesToRemove.get(entryMapping.getId());
        if (list == null) {
            list = handlerManager.filterAttributes(session, partition, entry, requestedAttributes, allRegularAttributes, allOpAttributes);
            attributesToRemove.put(entryMapping.getId(), list);
        }
        handlerManager.removeAttributes(entry, list);

        if (debug) {
            log.debug("After: "+entry.getDn());
            entry.getAttributeValues().print();
        }

        super.add(entry);
    }

    public void setResult(EntryMapping entryMapping, LDAPException exception) {
        results.put(entryMapping.getId(), exception);
    }

    public void close() throws Exception {
        if (results.size() < entryMappings.size()) return;

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

        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Found successes: "+successes);
        }

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
