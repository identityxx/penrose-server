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
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.SourceCacheManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.connection.Connection;

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

        connector = penrose.getConnector();
        sourceCacheManager = connector.getSourceCacheManager();
        engine = penrose.getEngine();
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
            pollChanges(sourceConfig);
        }
    }

    public void reloadExpired(Partition partition, SourceConfig sourceConfig) throws Exception {

        Map map = sourceCacheManager.getExpired(partition, sourceConfig);

        log.debug("Reloading expired caches...");

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues av = (AttributeValues)map.get(pk);
            log.debug(" - "+pk+": "+av);
        }

        PenroseSearchControls sc = new PenroseSearchControls();
        PenroseSearchResults list = new PenroseSearchResults();
        connector.retrieve(partition, sourceConfig, map.keySet(), sc, list);
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
            Row pk = (Row)sr.next();

            Number changeNumber = (Number)pk.remove("changeNumber");
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

            sourceCacheManager.remove(partition, sourceConfig, pk);

            for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)i.next();
                remove(entryMapping, sourceConfig, pk);
            }
        }

        sourceCacheManager.setLastChangeNumber(partition, sourceConfig, lastChangeNumber);

        PenroseSearchControls sc = new PenroseSearchControls();
        PenroseSearchResults list = new PenroseSearchResults();
        connector.retrieve(partition, sourceConfig, pks, sc, list);
        list.close();

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            for (Iterator j=entryMappings.iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                add(entryMapping, sourceConfig, pk);
            }
        }
    }

    public void add(EntryMapping entryMapping, SourceConfig sourceConfig, Row pk) throws Exception {

        log.debug("Adding entry cache for "+entryMapping.getDn());

        SourceMapping sourceMapping = engine.getPartitionManager().getPrimarySource(partition, entryMapping);
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
        AttributeValues attributeValues = engine.computeAttributeValues(entryMapping, sourceValues, interpreter);

        log.debug("Attribute values:");
        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);
            log.debug(" - "+name+": "+values);
        }

        final Row rdn = entryMapping.getRdn(attributeValues);
        EntryMapping parentMapping = partition.getParent(entryMapping);

        Handler handler = penrose.getHandler();
        EntryCache entryCache = handler.getEntryCache();

        PenroseSearchResults parentDns = new PenroseSearchResults();

        parentDns.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    String parentDn = (String)event.getObject();
                    String dn = rdn+","+parentDn;

                    log.debug("Adding "+dn);

                    PenroseSession adminSession = penrose.newSession();
                    adminSession.setBindDn(penrose.getPenroseConfig().getRootDn());

                    PenroseSearchResults sr = new PenroseSearchResults();

                    PenroseSearchControls sc = new PenroseSearchControls();
                    sc.setScope(PenroseSearchControls.SCOPE_SUB);

                    adminSession.search(
                            dn,
                            "(objectClass=*)",
                            sc,
                            sr
                    );

                    while (sr.hasNext()) sr.next();

                    adminSession.close();

                } catch (Exception e) {
                    log.debug(e.getMessage(), e);
                }
            }
        });

        entryCache.update(partition, parentMapping, parentDns);
    }

    public void remove(
            final EntryMapping entryMapping,
            final SourceConfig sourceConfig,
            final Row pk
    ) throws Exception {

        log.debug("Removing entry cache for "+entryMapping.getDn());

        Handler handler = penrose.getHandler();
        final EntryCache entryCache = handler.getEntryCache();

        PenroseSearchResults dns = new PenroseSearchResults();
        dns.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    String dn = (String)event.getObject();
                    entryCache.remove(partition, entryMapping, dn);

                } catch (Exception e) {
                    log.debug(e.getMessage(), e);
                }
            }
        });

        entryCache.search(partition, entryMapping, sourceConfig, pk, dns);
    }

}
