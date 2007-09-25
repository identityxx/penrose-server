package org.safehaus.penrose.handler.simple;

import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.Partitions;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.*;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SimpleHandler extends Handler {

    public SimpleHandler() throws Exception {
    }

    public String getEngineName() {
        return "SIMPLE";
    }

    public void add(
            Session session,
            Partition partition,
            Entry entry,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        Attributes attributes = request.getAttributes();
        Collection<Object> values = attributes.getValues("objectClass");

        Collection objectClasses = entry.getObjectClasses();
        boolean childHasObjectClass = false;

        for (Iterator i = objectClasses.iterator(); !childHasObjectClass && i.hasNext(); ) {
            String oc = (String)i.next();

            for (Object value : values) {
                String objectClass = (String) value;
                if (childHasObjectClass = oc.equalsIgnoreCase(objectClass)) break;
            }
        }

        if (!childHasObjectClass) {
            throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
        }

        super.add(session, partition, entry, request, response);
    }

    public void search(
            final Session session,
            final Entry base,
            final Entry entry,
            SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        DN baseDn = request.getDn();

        if (debug) {
            log.debug("Base DN: "+baseDn);
            log.debug("Entry mapping: "+entry.getDn());
        }

        Partitions partitions = penroseContext.getPartitions();
        int scope = request.getScope();
        if (scope == SearchRequest.SCOPE_BASE
                || scope == SearchRequest.SCOPE_SUB
                || scope == SearchRequest.SCOPE_ONE && entry.getParent() == base
                ) {

            SearchResponse sr = new SearchResponse() {
                 public void add(SearchResult searchResult) throws Exception {
                     response.add(searchResult);
                 }
             };

            super.performSearch(
                    session,
                    base,
                    entry,
                    sourceValues,
                    request,
                    sr
            );
        }

        if (scope == SearchRequest.SCOPE_ONE && entry == base
                || scope == SearchRequest.SCOPE_SUB) {

            Collection<Entry> children = entry.getChildren();

            for (Entry child : children) {
                Handler handler = partitions.getHandler(partition, child);

                handler.search(
                        session,
                        base,
                        child,
                        sourceValues,
                        request,
                        response
                );
            }
        }
    }
}
