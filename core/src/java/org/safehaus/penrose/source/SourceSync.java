package org.safehaus.penrose.source;

import org.safehaus.penrose.changelog.ChangeLogUtil;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.jdbc.JDBCClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.StringTokenizer;

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

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private SourceSyncConfig sourceSyncConfig;
    private Partition partition;

    private Source source;
    private Source changeLog;
    private Source tracker;
    private String user;

    private Collection<Source> destinations          = new ArrayList<Source>();
    private Collection<ChangeLogUtil> changeLogUtils = new ArrayList<ChangeLogUtil>();

    public SourceSync() {
    }

    public void init() throws Exception {

        String sourceName = sourceSyncConfig.getName();
        String destinationNames = sourceSyncConfig.getDestinations();
        //String sourceName = sourceSyncConfig.getParameter(SOURCE);
        //log.debug("Source: "+ sourceName);

        String changeLogName = sourceSyncConfig.getParameter(CHANGELOG);
        //log.debug("Change Log: "+changeLogName);

        String trackerName = sourceSyncConfig.getParameter(TRACKER);
        if (trackerName == null) trackerName = DEFAULT_TRACKER;
        //log.debug("Tracker: "+trackerName);

        user = (String) sourceSyncConfig.getParameter(USER);
        //log.debug("User: "+user);

        SourceManager sourceManager = penroseContext.getSourceManager();

        source = sourceManager.getSource(partition, sourceName);

        if (changeLogName != null) {
            changeLog = sourceManager.getSource(partition, changeLogName);
            if (changeLog == null) throw new Exception("Change log "+changeLogName+" not found.");

            tracker = sourceManager.getSource(partition, trackerName);
            if (tracker == null) throw new Exception("Tracker "+trackerName+" not found.");
        }

        StringTokenizer st = new StringTokenizer(destinationNames, ", ");
        while (st.hasMoreTokens()) {
            String destinationName = st.nextToken();

            Source destination = sourceManager.getSource(partition, destinationName);
            if (destination == null) throw new Exception("Source "+destinationName+" not found.");

            destinations.add(destination);

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
    }

    public void start() {
        if (tracker != null) {
            try {
                tracker.create();
            } catch (Exception e) {
                log.debug("Failed to create "+tracker.getName()+": "+e.getMessage());
            }
        }
    }

    public void stop() {
    }

    public ChangeLogUtil createChangeLogUtil() throws Exception {
        return null;
    }

    public Source createTemporarySource(Source dest) {
        String tableName = dest.getParameter(JDBCClient.TABLE);

        Source snapshot = (Source)dest.clone();
        snapshot.setName(dest.getName()+"_tmp");
        snapshot.setParameter(JDBCClient.TABLE, tableName+"_tmp");
        return snapshot;
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
            for (Iterator i=changeLogUtils.iterator(); i.hasNext(); ) {
                ChangeLogUtil changeLogUtil = (ChangeLogUtil)i.next();
                changeLogUtil.update();
            }
        }
    }

    public void update() throws Exception {

        Collection<Source> tmps = new ArrayList<Source>();

        for (Iterator i=destinations.iterator(); i.hasNext(); ) {
            Source destination = (Source)i.next();

            Source tmp = createTemporarySource(destination);
            tmps.add(tmp);

            try {
                tmp.create();
            } catch (Exception e) {
                log.error("Failed to create "+tmp.getName()+": "+e.getMessage());
                tmp.drop();
                tmp.create();
            }
        }

        load(tmps);

        Iterator i = destinations.iterator();
        Iterator j = tmps.iterator();

        while (i.hasNext() && j.hasNext()) {
            Source dest = (Source)i.next();
            Source tmp = (Source)j.next();

            try {
                dest.drop();
            } catch (Exception e) {
                log.debug("Failed to drop "+dest.getName()+": "+e.getMessage());
            }

            tmp.rename(dest);
        }

        log.debug("Source synchronization completed.");
    }

    public void load() throws Exception {
        load(destinations);
    }

    public void load(Collection<Source> targets) throws Exception {

        Interpreter interpreter = penroseContext.getInterpreterManager().newInstance();

        SearchRequest sourceRequest = new SearchRequest();
        SearchResponse<SearchResult> sourceResponse = new SourceSyncSearchResponse(
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
        for (Iterator i=destinations.iterator(); i.hasNext(); ) {
            Source destination = (Source)i.next();
            destination.create();
        }
    }

    public void clean() throws Exception {
        for (Iterator i=destinations.iterator(); i.hasNext(); ) {
            Source destination = (Source)i.next();
            destination.clean();
        }
    }

    public void drop() throws Exception {
        for (Iterator i=destinations.iterator(); i.hasNext(); ) {
            Source destination = (Source)i.next();
            destination.drop();
        }
    }

    public void status() throws Exception {
        for (Iterator i=destinations.iterator(); i.hasNext(); ) {
            Source destination = (Source)i.next();
            destination.status();
        }
    }
}
