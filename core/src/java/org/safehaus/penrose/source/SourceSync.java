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
package org.safehaus.penrose.source;

import org.safehaus.penrose.changelog.ChangeLogUtil;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.jdbc.JDBCClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SourceSync {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static String SOURCE              = "source";
    public final static String CHANGELOG           = "changelog";

    public final static String TRACKER             = "tracker";
    public final static String DEFAULT_TRACKER     = "tracker";

    public final static String TIMESTAMP           = "timestamp";

    public final static String USER                = "user";

    private SourceSyncConfig sourceSyncConfig;
    private SourceSyncContext sourceSyncContext;

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private Partition partition;

    private Source source;
    private Source changeLog;
    private Source tracker;
    private String user;

    private Collection<Source> destinations          = new ArrayList<Source>();
    private Collection<ChangeLogUtil> changeLogUtils = new ArrayList<ChangeLogUtil>();

    public SourceSync() {
    }

    public void init(SourceSyncConfig sourceSyncConfig, SourceSyncContext sourceSyncContext) throws Exception {

        this.sourceSyncConfig = sourceSyncConfig;
        this.sourceSyncContext = sourceSyncContext;

        this.partition = sourceSyncContext.getPartition();

        PartitionContext partitionContext = partition.getPartitionContext();
        this.penroseConfig = partitionContext.getPenroseConfig();
        this.penroseContext = partitionContext.getPenroseContext();

        String sourceName = sourceSyncConfig.getName();
        Collection<String> destinations = sourceSyncConfig.getDestinations();
        //String sourceName = sourceSyncConfig.getParameter(SOURCE);
        //log.debug("Source: "+ sourceName);

        String changeLogName = sourceSyncConfig.getParameter(CHANGELOG);
        //log.debug("Change Log: "+changeLogName);

        String trackerName = sourceSyncConfig.getParameter(TRACKER);
        if (trackerName == null) trackerName = DEFAULT_TRACKER;
        //log.debug("Tracker: "+trackerName);

        user = sourceSyncConfig.getParameter(USER);
        //log.debug("User: "+user);

        source = partition.getSource(sourceName);

        if (changeLogName != null) {
            changeLog = partition.getSource(changeLogName);
            if (changeLog == null) throw new Exception("Change log "+changeLogName+" not found.");

            tracker = partition.getSource(trackerName);
            if (tracker == null) throw new Exception("Tracker "+trackerName+" not found.");
        }

        for (String destinationName : destinations) {

            Source destination = partition.getSource(destinationName);
            if (destination == null) throw new Exception("Source "+destinationName+" not found.");

            this.destinations.add(destination);

            if (changeLogName != null) {
                ChangeLogUtil changeLogUtil = createChangeLogUtil();
                changeLogUtil.setSource(source);
                changeLogUtil.setDestination(destination);
                changeLogUtil.setChangeLog(changeLog);
                changeLogUtil.setTracker(tracker);
                changeLogUtil.setUser(user);

                changeLogUtils.add(changeLogUtil);
            }
        }

        if (tracker != null) {
            try {
                tracker.create();
            } catch (Exception e) {
                log.debug("Failed to create "+tracker.getName()+": "+e.getMessage());
            }
        }

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public ChangeLogUtil createChangeLogUtil() throws Exception {
        return null;
    }

    public Source createTemporarySource(Source dest) throws Exception {
        String tableName = dest.getParameter(JDBCClient.TABLE);

        Source tmp = (Source)dest.clone();
        tmp.setName(dest.getName()+"_tmp");
        tmp.setParameter(JDBCClient.TABLE, tableName+"_tmp");

        return tmp;
    }

    public String getName() {
        return sourceSyncConfig.getName();
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Collection<Source> getDestinations() {
        return destinations;
    }

    public void setDestinations(Collection<Source> destinations) {
        if (this.destinations == destinations) return;
        this.destinations.clear();
        this.destinations.addAll(destinations);
    }

    public Collection<ChangeLogUtil> getChangeLogUtils() {
        return changeLogUtils;
    }

    public void addChangeLogUtil(ChangeLogUtil changeLogUtil) {
        changeLogUtils.add(changeLogUtil);
    }

    public void setChangeLogUtil(Collection<ChangeLogUtil> changeLogUtils) {
        if (this.changeLogUtils == changeLogUtils) return;
        this.changeLogUtils.clear();
        this.changeLogUtils.addAll(changeLogUtils);
    }

    public void synchronize() throws Exception {
        if (changeLog == null) {
            update();

        } else {
            for (ChangeLogUtil changeLogUtil : changeLogUtils) {
                changeLogUtil.update();
            }
        }
    }

    public void update() throws Exception {

        Collection<Source> tmps = new ArrayList<Source>();

        for (Source destination : destinations) {

            Source tmp = createTemporarySource(destination);
            tmps.add(tmp);

            try {
                tmp.create();
            } catch (Exception e) {
                log.error("Failed to create " + tmp.getName() + ": " + e.getMessage());
                tmp.drop();
                tmp.create();
            }
        }

        load(tmps);

        Iterator i = destinations.iterator();
        Iterator j = tmps.iterator();

        while (i.hasNext() && j.hasNext()) {
            Source target = (Source)i.next();
            Source tmp = (Source)j.next();

            try {
                target.drop();
            } catch (Exception e) {
                log.debug("Failed to drop "+target.getName()+": "+e.getMessage());
            }

            try {
                tmp.rename(target);
            } catch (Exception e) {
                log.debug("Failed to rename "+tmp.getName()+": "+e.getMessage());
            }
        }

        log.debug("Source synchronization completed.");
    }

    public void load() throws Exception {
        load(destinations);
    }

    public void load(Collection<Source> targets) throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        SearchRequest sourceRequest = new SearchRequest();
        SearchResponse sourceResponse = new SourceSyncSearchResponse(
                source,
                targets,
                interpreter
        );

        source.search(sourceRequest, sourceResponse);
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public SourceSyncConfig getSourceSyncConfig() {
        return sourceSyncConfig;
    }

    public void setSourceSyncConfig(SourceSyncConfig sourceSyncConfig) {
        this.sourceSyncConfig = sourceSyncConfig;
    }

    public Source getChangeLog() {
        return changeLog;
    }

    public void setChangeLog(Source changeLog) {
        this.changeLog = changeLog;
    }

    public Source getTracker() {
        return tracker;
    }

    public void setTracker(Source tracker) {
        this.tracker = tracker;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public void create() throws Exception {
        for (Source destination : destinations) {
            destination.create();
        }
    }

    public void clean() throws Exception {
        for (Source destination : destinations) {
            destination.clear();
        }
    }

    public void drop() throws Exception {
        for (Source destination : destinations) {
            destination.drop();
        }
    }

    public void status() throws Exception {
        for (Source destination : destinations) {
            destination.status();
        }
    }

    public SourceSyncContext getSourceSyncContext() {
        return sourceSyncContext;
    }

    public void setSourceSyncContext(SourceSyncContext sourceSyncContext) {
        this.sourceSyncContext = sourceSyncContext;
    }
}
