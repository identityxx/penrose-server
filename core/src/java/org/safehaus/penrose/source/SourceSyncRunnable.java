package org.safehaus.penrose.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi S. Dewata
 */
public class SourceSyncRunnable implements Runnable {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected SourceSyncModule module;

    protected boolean running = true;

    public SourceSyncRunnable(SourceSyncModule module) {
        this.module = module;
    }

    public void run() {

        while (running) {

            try {
                Thread.sleep(module.getInterval() * 1000);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            if (!running) break;

            try {
                module.process();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void stop() {
        running = false;
    }
}
