package org.safehaus.penrose.handler.proxy;

import org.safehaus.penrose.handler.DefaultHandler;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.pipeline.Pipeline;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class ProxyHandler extends DefaultHandler {

    public ProxyHandler() throws Exception {
    }

    public Engine getEngine(EntryMapping entryMapping) {
        return engineManager.getEngine("PROXY");
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

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug("Base DN: "+baseDn);
            log.debug("Entry mapping: "+entryMapping.getDn());
        }

        Engine engine = getEngine(entryMapping);

        if (engine == null) {
            log.debug("Engine "+entryMapping.getEngineName()+" not found.");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        Pipeline sr = new Pipeline(results);

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
}
