/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.connector;

import org.safehaus.penrose.service.Service;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.ConnectionConfig;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.PenroseServer;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.handler.SessionHandler;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.SourceCache;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.Engine;
import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPSearchConstraints;

import java.util.Collection;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class PollingConnectorService extends Service {

    public final static String INTERVAL   = "interval";

    public final static int DEFAULT_INTERVAL = 5; // seconds

    public int interval; // 1 second

    public Connector connector;
    public SourceCache sourceCache;
    public Engine engine;
    public SessionHandler sessionHandler;

    public PollingConnectorRunnable runnable;

    public void init() throws Exception {

        log.debug("Initializing PollingConnectorService");
        ServiceConfig serviceConfig = getServiceConfig();

        String s = serviceConfig.getParameter(INTERVAL);
        interval = s == null ? DEFAULT_INTERVAL : Integer.parseInt(s);
        log.debug("Interval: "+interval);

        PenroseServer penroseServer = getPenroseServer();
        Penrose penrose = penroseServer.getPenrose();

        connector = penrose.getConnector();
        sourceCache = connector.getSourceCache();
        engine = penrose.getEngine();
        sessionHandler = penrose.getSessionHandler();
    }

    public void start() throws Exception {
        runnable = new PollingConnectorRunnable(this);

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public void stop() throws Exception {
        runnable.stop();
    }

    public void process(Partition partition) throws Exception {

        //log.debug("Refreshing cache ...");

        Collection sourceDefinitions = partition.getSourceConfigs();
        for (Iterator i=sourceDefinitions.iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();
/*
            String s = sourceConfig.getParameter(SourceConfig.AUTO_REFRESH);
            boolean autoRefresh = s == null ? SourceConfig.DEFAULT_AUTO_REFRESH : new Boolean(s).booleanValue();

            if (!autoRefresh) continue;
*/
            String s = sourceConfig.getParameter(SourceConfig.REFRESH_METHOD);
            String refreshMethod = s == null ? SourceConfig.DEFAULT_REFRESH_METHOD : s;

            //if (SourceConfig.POLL_CHANGES.equals(refreshMethod)) {
                pollChanges(sourceConfig);

            //} else { // if (SourceConfig.RELOAD_EXPIRED.equals(refreshMethod)) {
            //    reloadExpired(sourceConfig);
            //}
        }
    }

    public void reloadExpired(SourceConfig sourceConfig) throws Exception {

        Map map = sourceCache.getExpired(sourceConfig);

        log.debug("Reloading expired caches...");

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues av = (AttributeValues)map.get(pk);
            log.debug(" - "+pk+": "+av);
        }

        connector.retrieve(sourceConfig, map.keySet());
    }

    public void pollChanges(SourceConfig sourceConfig) throws Exception {

        int lastChangeNumber = sourceCache.getLastChangeNumber(sourceConfig);

        Connection connection = connector.getConnection(sourceConfig.getConnectionName());
        PenroseSearchResults sr = connection.getChanges(sourceConfig, lastChangeNumber);
        if (!sr.hasNext()) return;

        ConnectionConfig connectionConfig = connection.getConnectionConfig();
        String user = connectionConfig.getParameter("user");

        //CacheConfig cacheConfig = penroseConfig.getSourceCacheConfig();
        //String user = cacheConfig.getParameter("user");

        Partition partition = connector.getPartitionManager().getPartition(sourceConfig);
        Collection entryMappings = partition.getEntryMappings(sourceConfig);

        Collection pks = new HashSet();

        log.debug("Synchronizing changes in "+sourceConfig.getName()+":");
        while (sr.hasNext()) {
            Row pk = (Row)sr.next();

            Integer changeNumber = (Integer)pk.remove("changeNumber");
            Object changeTime = pk.remove("changeTime");
            String changeAction = (String)pk.remove("changeAction");
            String changeUser = (String)pk.remove("changeUser");

            log.debug(" - "+pk+": "+changeAction+" ("+changeTime+")");

            lastChangeNumber = changeNumber.intValue();

            if (user != null && user.equals(changeUser)) {
                log.debug("Skip changes made by "+user);
                continue;
            }

            if ("DELETE".equals(changeAction)) {
                pks.remove(pk);
            } else {
                pks.add(pk);
            }

            sourceCache.remove(sourceConfig, pk);

            for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)i.next();
                remove(partition, entryMapping, sourceConfig, pk);
            }
        }

        sourceCache.setLastChangeNumber(sourceConfig, lastChangeNumber);

        connector.retrieve(sourceConfig, pks);

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            for (Iterator j=entryMappings.iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                add(partition, entryMapping, sourceConfig, pk);
            }
        }
    }

    public void add(Partition partition, EntryMapping entryMapping, SourceConfig sourceConfig, Row pk) throws Exception {

        log.debug("Adding entry cache for "+entryMapping.getDn());

        SourceMapping sourceMapping = engine.getPrimarySource(entryMapping);
        log.debug("Primary source: "+sourceMapping.getName()+" ("+sourceMapping.getSourceName()+")");

        AttributeValues sv = (AttributeValues)sourceCache.get(sourceConfig, pk);

        AttributeValues sourceValues = new AttributeValues();
        sourceValues.set(sourceMapping.getName(), sv);

        log.debug("Source values:");
        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);
            log.debug(" - "+name+": "+values);
        }

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();
        AttributeValues attributeValues = engine.computeAttributeValues(entryMapping, sourceValues, interpreter);

        log.debug("Attribute values:");
        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);
            log.debug(" - "+name+": "+values);
        }

        Row rdn = entryMapping.getRdn(attributeValues);
        EntryMapping parentMapping = partition.getParent(entryMapping);

        EntryCache entryCache = engine.getEntryCache();

        Collection parentDns = entryCache.search(partition, parentMapping);
        for (Iterator i=parentDns.iterator(); i.hasNext(); ) {
            String parentDn = (String)i.next();
            String dn = rdn+","+parentDn;

            log.debug("Adding "+dn);

            PenroseSearchResults sr = sessionHandler.search(
                    null,
                    dn,
                    LDAPConnection.SCOPE_SUB,
                    LDAPSearchConstraints.DEREF_NEVER,
                    "(objectClass=*)",
                    null
            );

            while (sr.hasNext()) sr.next();
        }
    }

    public void remove(Partition partition, EntryMapping entryMapping, SourceConfig sourceConfig, Row pk) throws Exception {

        log.debug("Removing entry cache for "+entryMapping.getDn());

        EntryCache entryCache = engine.getEntryCache();

        Collection dns = entryCache.search(partition, entryMapping, sourceConfig, pk);
        for (Iterator i=dns.iterator(); i.hasNext(); ) {
            String dn = (String)i.next();

            entryCache.remove(partition, entryMapping, dn);
        }
    }

}
