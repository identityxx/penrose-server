/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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

import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.ConnectionConfig;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.SourceCacheManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.Collection;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class PollingConnectorModule extends Module {

    public final static String INTERVAL   = "interval";

    public final static int DEFAULT_INTERVAL = 5; // seconds

    public int interval; // 1 second

    public Connector connector;
    public SourceCacheManager sourceCacheManager;
    public Engine engine;

    public PollingConnectorRunnable runnable;

    public void init() throws Exception {

        log.debug("Initializing PollingConnectorModule");

        String s = getParameter(INTERVAL);
        interval = s == null ? DEFAULT_INTERVAL : Integer.parseInt(s);
        log.debug("Interval: "+interval);

        ConnectorConfig connectorConfig = penroseConfig.getConnectorConfig();
        ConnectorManager connectorManager = penroseContext.getConnectorManager();
        connector = connectorManager.getConnector(connectorConfig.getName());
        
        sourceCacheManager = connector.getSourceCacheManager();

        EngineManager engineManager = penroseContext.getEngineManager();
        engine = engineManager.getEngine("DEFAULT");
    }

    public void start() throws Exception {
        runnable = new PollingConnectorRunnable(this);

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public void stop() throws Exception {
        runnable.stop();
    }

    public void process() throws Exception {

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

        Map map = sourceCacheManager.getExpired(partition, sourceConfig);

        log.debug("Reloading expired caches...");

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            RDN pk = (RDN)i.next();
            AttributeValues av = (AttributeValues)map.get(pk);
            log.debug(" - "+pk+": "+av);
        }

        PenroseSearchControls sc = new PenroseSearchControls();
        PenroseSearchResults list = new PenroseSearchResults();
        //connector.retrieve(partition, sourceConfig, map.keySet(), sc, list);
        list.close();
    }

    public void pollChanges(SourceConfig sourceConfig) throws Exception {

        int lastChangeNumber = sourceCacheManager.getLastChangeNumber(partition, sourceConfig);

        Connection connection = connector.getConnection(partition, sourceConfig.getConnectionName());
        PenroseSearchResults sr = connection.getChanges(sourceConfig, lastChangeNumber);
        if (!sr.hasNext()) return;

        ConnectionConfig connectionConfig = connection.getConnectionConfig();
        String user = connectionConfig.getParameter("user");

        //CacheConfig cacheConfig = penroseConfig.getSourceCacheConfig();
        //String user = cacheConfig.getParameter("user");

        Collection entryMappings = partition.getEntryMappings(sourceConfig);

        Collection pks = new HashSet();

        log.debug("Synchronizing changes in "+sourceConfig.getName()+":");
        while (sr.hasNext()) {
            RDN pk = (RDN)sr.next();

            RDNBuilder rb = new RDNBuilder();
            rb.set(pk);

            Number changeNumber = (Number)rb.remove("changeNumber");
            Object changeTime = rb.remove("changeTime");
            String changeAction = (String)rb.remove("changeAction");
            String changeUser = (String)rb.remove("changeUser");
            RDN newPk = rb.toRdn();

            log.debug(" - "+newPk+": "+changeAction+" ("+changeTime+")");

            lastChangeNumber = changeNumber.intValue();

            if (user != null && user.equals(changeUser)) {
                log.debug("Skip changes made by "+user);
                continue;
            }

            pks.add(newPk);

            log.debug("Removing source cache "+newPk);
            sourceCacheManager.remove(partition, sourceConfig, newPk);

            for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)i.next();
                log.debug("Removing entries in "+entryMapping.getDn()+" with "+newPk);
                remove(entryMapping, sourceConfig, newPk);
            }
        }

        log.debug("Reloading data for "+pks);
        PenroseSearchControls sc = new PenroseSearchControls();
        PenroseSearchResults list = new PenroseSearchResults();
        //connector.retrieve(partition, sourceConfig, pks, sc, list);
        list.close();
        while (list.hasNext()) list.next();

        log.debug("Creating entries with "+pks);
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            RDN pk = (RDN)i.next();

            for (Iterator j=entryMappings.iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                add(entryMapping, sourceConfig, pk);
            }
        }

        log.debug("Invalidating entry cache");
        HandlerManager handlerManager = penroseContext.getHandlerManager();
        Handler handler = handlerManager.getHandler("DEFAULT");
        EntryCache entryCache = handler.getEntryCache();
        entryCache.invalidate(partition);

        sourceCacheManager.setLastChangeNumber(partition, sourceConfig, lastChangeNumber);
    }

    public void add(
            EntryMapping entryMapping,
            SourceConfig sourceConfig,
            RDN pk
    ) throws Exception {

        log.debug("Adding entry cache for "+entryMapping.getDn());

        SourceMapping sourceMapping = engine.getPrimarySource(entryMapping);
        log.debug("Primary source: "+sourceMapping.getName()+" ("+sourceMapping.getSourceName()+")");

        AttributeValues sv = (AttributeValues)sourceCacheManager.get(partition, sourceConfig, pk);

        AttributeValues sourceValues = new AttributeValues();
        sourceValues.set(sourceMapping.getName(), sv);

        log.debug("Source values:");
        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);
            log.debug(" - "+name+": "+values);
        }

        Interpreter interpreter = engine.getInterpreterManager().newInstance();
        Collection dns = engine.computeDns(partition, interpreter, entryMapping, sourceValues);

        SessionManager sessionManager = penroseContext.getSessionManager();
        PenroseSession session = sessionManager.newSession();

        session.setBindDn(penroseConfig.getRootDn());

        PenroseSearchControls sc = new PenroseSearchControls();

        for (Iterator i=dns.iterator(); i.hasNext(); ) {
            DN dn = (DN)i.next();

            PenroseSearchResults sr = new PenroseSearchResults();
            session.search(dn, "(objectClass=*)", sc, sr);
            while (sr.hasNext()) sr.next();
        }
    }

    public void remove(
            EntryMapping entryMapping,
            SourceConfig sourceConfig,
            RDN pk
    ) throws Exception {

        log.debug("Removing entry cache for "+entryMapping.getDn());

        SourceMapping sourceMapping = engine.getPrimarySource(entryMapping);
        log.debug("Primary source: "+sourceMapping.getName()+" ("+sourceMapping.getSourceName()+")");

        AttributeValues sv = (AttributeValues)sourceCacheManager.get(partition, sourceConfig, pk);

        AttributeValues sourceValues = new AttributeValues();
        sourceValues.set(sourceMapping.getName(), sv);

        log.debug("Source values:");
        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);
            log.debug(" - "+name+": "+values);
        }

        Interpreter interpreter = engine.getInterpreterManager().newInstance();
        Collection dns = engine.computeDns(partition, interpreter, entryMapping, sourceValues);

        HandlerManager handlerManager = penroseContext.getHandlerManager();
        Handler handler = handlerManager.getHandler("DEFAULT");
        EntryCache entryCache = handler.getEntryCache();

        for (Iterator i=dns.iterator(); i.hasNext(); ) {
            DN dn = (DN)i.next();
            entryCache.remove(partition, entryMapping, dn);
        }
    }
}
