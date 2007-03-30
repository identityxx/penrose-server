package org.safehaus.penrose.cache;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.source.Source;

/**
 * @author Endi S. Dewata
 */
public abstract class CacheModule extends Module {

    public final static String SOURCE        = "source";
    public final static String CACHE         = "cache";

    public final static String INTERVAL      = "interval";
    public final static int DEFAULT_INTERVAL = 30; // seconds

    protected String sourceName;
    protected String cacheName;
    protected int interval; // second

    protected CacheRunnable runnable;

    protected SourceManager sourceManager;
    protected Source source;
    protected Source cache;

    public void init() throws Exception {

        log.debug("Initializing SourceCacheModule");

        sourceName = getParameter(SOURCE);
        log.debug("Source: "+ sourceName);

        cacheName = getParameter(CACHE);
        log.debug("Cache: "+ cacheName);

        String s = getParameter(INTERVAL);
        interval = s == null ? DEFAULT_INTERVAL : Integer.parseInt(s);
        log.debug("Interval: "+ getInterval());

        sourceManager = penroseContext.getSourceManager();
        source = sourceManager.getSource(partition.getName(), sourceName);
        cache = sourceManager.getSource(partition.getName(), cacheName);
    }

    public void start() throws Exception {

        runnable = new CacheRunnable(this);

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public void stop() throws Exception {
        runnable.stop();
    }

    public abstract void process() throws Exception;

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }
}
