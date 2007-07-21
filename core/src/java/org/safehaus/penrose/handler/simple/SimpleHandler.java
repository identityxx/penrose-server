package org.safehaus.penrose.handler.simple;

import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.ldap.*;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SimpleHandler extends Handler {

    public SimpleHandler() throws Exception {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {
        super.init(handlerConfig);
    }

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        Attributes attributes = request.getAttributes();
        Collection<Object> values = attributes.getValues("objectClass");

        Collection objectClasses = entryMapping.getObjectClasses();
        boolean childHasObjectClass = false;

        for (Iterator i = objectClasses.iterator(); !childHasObjectClass && i.hasNext(); ) {
            String oc = (String)i.next();

            for (Object value : values) {
                String objectClass = (String) value;
                if (childHasObjectClass = oc.equalsIgnoreCase(objectClass)) break;
            }
        }

        if (!childHasObjectClass) {
            throw ExceptionUtil.createLDAPException(LDAPException.OBJECT_CLASS_VIOLATION);
        }

        super.add(session, partition, entryMapping, request, response);
    }

    public void search(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        DN baseDn = request.getDn();

        if (debug) {
            log.debug("Base DN: "+baseDn);
            log.debug("Entry mapping: "+entryMapping.getDn());
        }

        int scope = request.getScope();
        if (scope == SearchRequest.SCOPE_BASE
                || scope == SearchRequest.SCOPE_SUB
                || scope == SearchRequest.SCOPE_ONE && partition.getMappings().getParent(entryMapping) == baseMapping
                ) {

            SearchResponse<SearchResult> sr = new SearchResponse<SearchResult>() {
                 public void add(SearchResult searchResult) throws Exception {
                     response.add(searchResult);
                 }
             };

            super.performSearch(
                    session,
                    partition,
                    baseMapping,
                    entryMapping,
                    request,
                    sr
            );
        }

        if (scope == SearchRequest.SCOPE_ONE && entryMapping == baseMapping
                || scope == SearchRequest.SCOPE_SUB) {

            Collection<EntryMapping> children = partition.getMappings().getChildren(entryMapping);

            for (EntryMapping childMapping : children) {
                Handler handler = handlerManager.getHandler(partition, childMapping);

                handler.search(
                        session,
                        partition,
                        baseMapping,
                        childMapping,
                        request,
                        response
                );
            }
        }
    }
}
