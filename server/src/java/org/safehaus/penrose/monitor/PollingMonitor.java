package org.safehaus.penrose.monitor;

/**
 * @author Endi Sukma Dewata
 */
public class PollingMonitor extends Monitor {

    public final static String INTERVAL = "interval";
    public final int DEFAULT_INTERVAL = 30; // seconds

    public int interval; // seconds

    public Thread thread;
    public boolean stopped;

    public PollingMonitor() {
    }

    public void init() throws Exception {
        super.init();

        String s = monitorConfig.getParameter(INTERVAL);
        interval = s == null ? DEFAULT_INTERVAL : Integer.parseInt(s);
    }

    public void start() throws Exception {

        log.debug("Starting thread for "+monitorConfig.getName()+" monitor.");

        thread = new Thread() {
            public void run() {
                try {
                    while (!stopped) {

                        Thread.sleep(interval * 1000);

                        try {
                            validate();
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }

                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        };

        thread.start();
    }

    public void validate() throws Exception {
    }

    public void stop() throws Exception {
        stopped = true;
    }
}