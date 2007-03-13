package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.pipeline.Pipeline;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPConnection;

import javax.naming.directory.*;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class DefaultHandler extends Handler {

    public DefaultHandler() throws Exception {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {
        super.init(handlerConfig);
    }

    public void add(PenroseSession session, Partition partition, EntryMapping entryMapping, DN dn, Attributes attributes) throws Exception {

        // -dlc- if the objectClass of the add Attributes does
        // not match any of the objectClasses of the entryMapping, there
        // is no sense trying to perform an add on this entryMapping
        Attribute at = attributes.get("objectClass");

        Collection objectClasses = entryMapping.getObjectClasses();
        boolean childHasObjectClass = false;

        for (Iterator i = objectClasses.iterator(); !childHasObjectClass && i.hasNext(); ) {
            String oc = (String)i.next();

            for (int j = 0; j < at.size(); j++) {
                String objectClass = (String) at.get(j);
                if (childHasObjectClass = oc.equalsIgnoreCase(objectClass)) break;
            }
        }

        if (!childHasObjectClass) {
            throw ExceptionUtil.createLDAPException(LDAPException.OBJECT_CLASS_VIOLATION);
        }

        super.add(session, partition, entryMapping, dn, attributes);
    }

    public void search(
            final PenroseSession session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Base DN: "+baseDn);
            log.debug("Entry mapping: "+entryMapping.getDn());
        }

        if (sc.getScope() == LDAPConnection.SCOPE_BASE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // base or subtree

            Pipeline sr = new Pipeline(results) {
                public void add(Object object) throws Exception {
                    Entry child = (Entry)object;

                    if (debug) log.debug("Checking filter "+filter+" on "+child.getDn());

                    if (!filterTool.isValid(child, filter)) { // Check LDAP filter
                        if (debug) log.debug("Entry \""+child.getDn()+"\" doesn't match search filter.");
                        return;
                    }

                    super.add(child);
                }
            };

            Engine engine = getEngine(entryMapping);

            if (engine == null) {
                if (debug) log.debug("Engine "+entryMapping.getEngineName()+" not found.");
                throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
            }

            AttributeValues sourceValues = new AttributeValues();

            engine.search(
                    session,
                    partition,
                    sourceValues,
                    entryMapping,
                    baseDn,
                    filter,
                    sc,
                    sr
            );
        }

        if (sc.getScope() == LDAPConnection.SCOPE_ONE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // one level or subtree

            Collection children = partition.getChildren(entryMapping);

            for (Iterator i = children.iterator(); i.hasNext();) {
                EntryMapping childMapping = (EntryMapping) i.next();
                Handler handler = handlerManager.getHandler(childMapping);

                handler.search(
                        session,
                        partition,
                        baseMapping,
                        childMapping,
                        baseDn,
                        filter,
                        sc,
                        results
                );
            }
        }
    }
}
