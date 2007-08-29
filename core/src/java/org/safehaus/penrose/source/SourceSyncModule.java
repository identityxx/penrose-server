package org.safehaus.penrose.source;

import org.safehaus.penrose.module.Module;

import java.util.StringTokenizer;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SourceSyncModule extends Module {

    public final static String SOURCE              = "source";

    public final static String INTERVAL            = "interval";
    public final static int DEFAULT_INTERVAL       = 30; // seconds

    public final static String INITIALIZE          = "initialize";
    public final static boolean DEFAULT_INITIALIZE = false;

    protected String sourceNames;
    protected int interval; // second
    protected boolean initialize;

    protected Collection<SourceSync> sourceSyncs = new ArrayList<SourceSync>();

    protected SourceSyncRunnable runnable;

    public void init() throws Exception {

        sourceNames = getParameter(SOURCE);
        log.debug("Source: "+sourceNames);

        String s = getParameter(INTERVAL);
        interval = s == null ? DEFAULT_INTERVAL : Integer.parseInt(s);
        log.debug("Interval: "+interval);

        s = getParameter(INITIALIZE);
        initialize = s == null ? DEFAULT_INITIALIZE : Boolean.parseBoolean(s);
        log.debug("Initialize: "+initialize);

        StringTokenizer st = new StringTokenizer(sourceNames, ", \t\n\r\f");
        while (st.hasMoreTokens()) {
            String sourceName = st.nextToken();

            SourceSync sourceSync = partition.getSourceSync(sourceName);
            if (sourceSync == null) {
                log.error("Source sync "+partition.getName()+"/"+sourceName+" not found.");
                continue;
            }

            sourceSyncs.add(sourceSync);
        }

        if (initialize) {
            log.warn("Initializing "+partition.getName()+"/"+sourceNames+".");
            for (SourceSync sourceSync : sourceSyncs) {
                sourceSync.update();
            }
        }

        runnable = new SourceSyncRunnable(this);

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public void destroy() throws Exception {
        runnable.stop();
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public void process() throws Exception {
        for (SourceSync sourceSync : sourceSyncs) {
            log.warn("Synchronizing " + partition.getName() + "/" + sourceSync.getName() + ".");
            sourceSync.synchronize();
        }
    }

    public boolean isInitialize() {
        return initialize;
    }

    public void setInitialize(boolean initialize) {
        this.initialize = initialize;
    }

    public String getSourceNames() {
        return sourceNames;
    }

    public void setSourceNames(String sourceNames) {
        this.sourceNames = sourceNames;
    }
}
