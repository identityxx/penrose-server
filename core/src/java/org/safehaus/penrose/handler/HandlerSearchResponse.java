package org.safehaus.penrose.handler;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.acl.ACLEvaluator;
import org.ietf.ldap.LDAPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class HandlerSearchResponse extends SearchResponse {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    SearchResponse response;

    Session session;
    Partition partition;

    ACLEvaluator aclEvaluator;

    Collection<String> requestedAttributes;
    boolean allRegularAttributes;
    boolean allOpAttributes;

    Collection<Entry> entries;
    Map<String,Collection<String>> attributesToRemove = new HashMap<String,Collection<String>>();
    Map<String,Exception> results = new HashMap<String,Exception>();

    public HandlerSearchResponse(
            SearchResponse parent,
            Session session,
            Partition partition,
            Collection<String> requestedAttributes,
            boolean allRegularAttributes,
            boolean allOpAttributes,
            Collection<Entry> entries
    ) {
        this.response = parent;
        this.session = session;
        this.partition = partition;

        this.aclEvaluator = partition.getAclEvaluator();

        this.requestedAttributes = requestedAttributes;
        this.allRegularAttributes = allRegularAttributes;
        this.allOpAttributes = allOpAttributes;

        this.entries = entries;
    }

    public void add(SearchResult searchResult) throws Exception {

        DN dn = searchResult.getDn();
        Entry entry = searchResult.getEntry();
        Attributes attributes = searchResult.getAttributes();

        if (!session.isRootUser()) {
            if (debug) log.debug("Checking read permission.");
            
            int rc = aclEvaluator.checkRead(session, partition, entry, dn);
            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Entry \""+searchResult.getDn()+"\" is not readable.");
                return;
            }
            partition.filterAttributes(session, dn, entry, attributes);
        }

        Collection<String> list = attributesToRemove.get(entry.getId());
        if (list == null) {
            list = partition.filterAttributes(session, searchResult, requestedAttributes, allRegularAttributes, allOpAttributes);
            attributesToRemove.put(entry.getId(), list);
        }

        if (!list.isEmpty()) {
            if (debug) log.debug("Removing attributes: "+list);
            partition.removeAttributes(attributes, list);
        }

        if (debug) {
            log.debug("Returning "+dn+":");
            attributes.print();
        }

        SearchResult result = new SearchResult(dn, attributes);

        response.add(result);
    }

    public void setResult(Entry entry, LDAPException exception) {
        results.put(entry.getId(), exception);
    }

    public void close() throws Exception {

        int count = entries.size() - results.size();

        if (count > 0) {
            if (debug) log.debug("Search thread ended. Waiting for "+count+" more.");
            return;
        } else {
            if (debug) log.debug("All search threads have ended.");
        }

        boolean successes = false;
        LDAPException noSuchObject = null;
        LDAPException exception = null;

        for (Entry entry : entries) {
            LDAPException ldapException = (LDAPException) results.get(entry.getId());

            int rc = ldapException.getResultCode();
            switch (rc) {
                case LDAP.SUCCESS:
                    successes = true;
                    break;

                case LDAP.NO_SUCH_OBJECT:
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
