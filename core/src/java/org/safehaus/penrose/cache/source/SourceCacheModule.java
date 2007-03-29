package org.safehaus.penrose.cache.source;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.source.Source;

/**
 * @author Endi S. Dewata
 */
public class SourceCacheModule extends Module {

    public final static String SOURCE        = "source";
    public final static String DESTINATION   = "destination";

    public final static String INTERVAL      = "interval";
    public final static int DEFAULT_INTERVAL = 30; // seconds

    public String sourceName;
    public String destinationName;
    public int interval; // second

    public SourceManager sourceManager;
    public SourceCacheRunnable runnable;

    public void init() throws Exception {

        log.debug("Initializing SourceCacheModule");

        sourceName = getParameter(SOURCE);
        destinationName = getParameter(DESTINATION);

        String s = getParameter(INTERVAL);
        interval = s == null ? DEFAULT_INTERVAL : Integer.parseInt(s);
        log.debug("Interval: "+interval);

        sourceManager = penroseContext.getSourceManager();
    }

    public void start() throws Exception {
        runnable = new SourceCacheRunnable(this);

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public void stop() throws Exception {
        runnable.stop();
    }

    public void process() throws Exception {

        boolean debug = log.isDebugEnabled();

        Source source = sourceManager.getSource(partition.getName(), sourceName);

        SearchRequest sourceRequest = new SearchRequest();
        SearchResponse sourceResponse = new SearchResponse();

        source.search(sourceRequest, sourceResponse);

        Source destination = sourceManager.getSource(partition.getName(), destinationName);

        SearchRequest destinationRequest = new SearchRequest();
        SearchResponse destinationResponse = new SearchResponse();

        destination.search(destinationRequest, destinationResponse);

        Entry sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;
        Entry destinationEntry = destinationResponse.hasNext() ? (Entry)destinationResponse.next() : null;

        while (sourceEntry != null && destinationEntry != null) {
            int c = sourceEntry.getDn().compareTo(destinationEntry.getDn());

            if (c  < 0) {
                if (debug) log.debug("Adding "+sourceEntry.getDn()+" to destination.");
                destination.add(sourceEntry.getDn(), sourceEntry.getAttributes());

                sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;

            } else if (c > 0) {
                if (debug) log.debug("Deleting "+destinationEntry.getDn()+" from destination.");
                destination.delete(destinationEntry.getDn());

                destinationEntry = destinationResponse.hasNext() ? (Entry)destinationResponse.next() : null;

            } else {
                sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;
                destinationEntry = destinationResponse.hasNext() ? (Entry)destinationResponse.next() : null;
            }
        }

        while (sourceEntry != null) {
            if (debug) log.debug("Adding "+sourceEntry.getDn()+" to destination.");
            destination.add(sourceEntry.getDn(), sourceEntry.getAttributes());

            sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;
        }

        while (destinationEntry != null) {
            if (debug) log.debug("Deleting "+destinationEntry.getDn()+" from destination.");
            destination.delete(destinationEntry.getDn());

            destinationEntry = destinationResponse.hasNext() ? (Entry)destinationResponse.next() : null;
        }
    }
}
