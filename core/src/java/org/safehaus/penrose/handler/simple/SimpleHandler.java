package org.safehaus.penrose.handler.simple;

import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.engine.Engine;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

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
        Collection values = attributes.getValues("objectClass");

        Collection objectClasses = entryMapping.getObjectClasses();
        boolean childHasObjectClass = false;

        for (Iterator i = objectClasses.iterator(); !childHasObjectClass && i.hasNext(); ) {
            String oc = (String)i.next();

            for (Iterator j = values.iterator(); j.hasNext(); ) {
                String objectClass = (String)j.next();
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
            final SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN baseDn = request.getDn();

        if (debug) {
            log.debug("Base DN: "+baseDn);
            log.debug("Entry mapping: "+entryMapping.getDn());
        }

        int scope = request.getScope();
        if (scope == LDAPConnection.SCOPE_BASE
                || scope == LDAPConnection.SCOPE_SUB
                || scope == LDAPConnection.SCOPE_ONE && partition.getParent(entryMapping) == baseMapping
                ) {

            Engine engine = getEngine(partition, entryMapping);

            AttributeValues sourceValues = new AttributeValues();

            engine.search(
                    session,
                    partition,
                    sourceValues,
                    entryMapping,
                    request,
                    response
            );
        }

        if (scope == LDAPConnection.SCOPE_ONE && entryMapping == baseMapping
                || scope == LDAPConnection.SCOPE_SUB) {

            Collection children = partition.getChildren(entryMapping);

            for (Iterator i = children.iterator(); i.hasNext();) {
                EntryMapping childMapping = (EntryMapping) i.next();
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
