package org.safehaus.penrose.cache.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi S. Dewata
 */
public class SourceCacheRunnable implements Runnable {

    Logger log = LoggerFactory.getLogger(getClass());

    private SourceCacheModule module;

    boolean running = true;

    public SourceCacheRunnable(SourceCacheModule module) {
        this.module = module;
    }

    public void run() {
        try {
            runImpl();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void runImpl() throws Exception {

        while (running) {
            Thread.sleep(module.interval * 1000);
            if (running) module.process();
        }

    }

    public void stop() {
        running = false;
    }
}
