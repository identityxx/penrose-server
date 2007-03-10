package org.safehaus.penrose.handler.proxy;

import org.safehaus.penrose.handler.SearchHandler;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.pipeline.Pipeline;
import org.ietf.ldap.LDAPConnection;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ProxySearchHandler extends SearchHandler {

    public ProxySearchHandler(Handler handler) {
        super(handler);
    }

    public void search(
            final PenroseSession session,
            final Partition partition,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Searching "+baseDn);
            log.debug("in mapping "+entryMapping.getDn());
        }

        AttributeValues sourceValues = new AttributeValues();

        searchBase(
                session,
                partition,
                sourceValues,
                entryMapping,
                baseDn,
                filter,
                sc,
                results
        );
	}

}
