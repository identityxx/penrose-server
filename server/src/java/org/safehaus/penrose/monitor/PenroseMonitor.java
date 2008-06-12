package org.safehaus.penrose.monitor;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.Penrose;

import java.util.Collection;
import java.util.Arrays;
import java.util.Enumeration;
import java.io.File;

/**
 * @author Endi Sukma Dewata
 */
public class PenroseMonitor {

    public static Logger log = Logger.getLogger(PenroseMonitor.class);
    public static org.slf4j.Logger errorLog = org.safehaus.penrose.log.Error.log;
    public static boolean debug = log.isDebugEnabled();

    File home;

    MonitorConfigManager monitorConfigManager;
    MonitorManager monitorManager;

    public PenroseMonitor(File home) throws Exception {
        this.home = home;
    }

    public void init() throws Exception {

        File monitorsDir = new File(home, "monitors");
        monitorConfigManager = new MonitorConfigManager(monitorsDir);

        monitorManager = new MonitorManager(monitorConfigManager);
        monitorManager.setHome(home);
        monitorManager.init();
    }

    public void start() throws Exception {

        log.debug("Starting Penrose Monitor...");

        log.debug("----------------------------------------------------------------------------------");

        for (String monitorName : monitorConfigManager.getAvailableMonitorNames()) {

            try {
                monitorManager.loadMonitorConfig(monitorName);
                monitorManager.startMonitor(monitorName);

            } catch (Exception e) {
                errorLog.error(e.getMessage(), e);
            }
        }

        log.fatal("Penrose Monitor is ready.");
    }

    public void stop() throws Exception {

        log.debug("----------------------------------------------------------------------------------");
        log.debug("Stopping Penrose Monitor...");

        for (String monitorName : monitorConfigManager.getAvailableMonitorNames()) {

            try {
                monitorManager.stopMonitor(monitorName);
                monitorManager.unloadMonitor(monitorName);

            } catch (Exception e) {
                errorLog.error(e.getMessage(), e);
            }
        }

        log.fatal("Penrose Monitor has been shutdown.");
    }

    public static void main(String[] args) throws Exception {
        try {
            Collection parameters = Arrays.asList(args);

            if (parameters.contains("-?") || parameters.contains("--help")) {
                System.out.println("Usage: org.safehaus.penrose.monitor.PenroseMonitor [OPTION]...");
                System.out.println();
                System.out.println("  -?, --help     display this help and exit");
                System.out.println("  -d             run in debug mode");
                System.out.println("  -v             run in verbose mode");
                System.exit(0);
            }

            File home = new File(System.getProperty("penrose.home"));

            File log4jXml = new File(home, "conf"+File.separator+"log4j.xml");

            if (log4jXml.exists()) {
                DOMConfigurator.configure(log4jXml.getAbsolutePath());
            }

            for (Enumeration e = Logger.getRootLogger().getLoggerRepository().getCurrentLoggers(); e.hasMoreElements(); ) {
                Logger logger = (Logger)e.nextElement();

                if (parameters.contains("-d")) {
                    if (logger.getAppender("console") != null) {
                        logger.setLevel(Level.DEBUG);
                        logger.removeAppender("console");
                        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
                        logger.addAppender(appender);
                    }

                } else if (parameters.contains("-v")) {
                    if (logger.getAppender("console") != null) {
                        logger.setLevel(Level.INFO);
                        logger.removeAppender("console");
                        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
                        logger.addAppender(appender);
                    }
                }
            }

            log.warn("Starting "+ Penrose.PRODUCT_NAME+" Monitor.");

            String javaVersion = System.getProperty("java.version");
            log.info("Java version: "+javaVersion);

            String javaVendor = System.getProperty("java.vendor");
            log.info("Java vendor: "+javaVendor);

            String javaHome = System.getProperty("java.home");
            log.info("Java home: "+javaHome);

            String userDir = System.getProperty("user.dir");
            log.info("Current directory: "+userDir);

            log.info("Penrose home: "+home);

            final PenroseMonitor penroseMonitor = new PenroseMonitor(home);
            penroseMonitor.init();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        penroseMonitor.stop();
                    } catch (Exception e) {
                        errorLog.error(e.getMessage(), e);
                    }
                }
            });

            penroseMonitor.start();

        } catch (Exception e) {
            String name = e.getClass().getName();
            name = name.substring(name.lastIndexOf(".")+1);
            log.error(e.getMessage(), e);
            log.error("Monitor failed to start: "+name+": "+e.getMessage());
            System.exit(1);
        }
    }
}
