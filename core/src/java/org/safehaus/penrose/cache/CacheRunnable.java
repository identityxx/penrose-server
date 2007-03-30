package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi S. Dewata
 */
public class CacheRunnable implements Runnable {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected CacheModule module;

    protected boolean running = true;

    public CacheRunnable(CacheModule module) {
        this.module = module;
    }

    public void run() {

        while (true) {

            if (!running) break;

            try {
                module.process();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            if (!running) break;

            try {
                Thread.sleep(module.getInterval() * 1000);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void stop() {
        running = false;
    }
}
