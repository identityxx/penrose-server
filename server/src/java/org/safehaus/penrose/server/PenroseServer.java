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
package org.safehaus.penrose.server;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.service.ServiceConfigManager;
import org.safehaus.penrose.service.ServiceManager;

import java.io.File;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseServer {

    public static Logger log = Logger.getLogger(PenroseServer.class);
    public static org.slf4j.Logger errorLog = org.safehaus.penrose.log.Error.log;
    public static boolean debug = log.isDebugEnabled();

    public static String PRODUCT_NAME;
    public static String PRODUCT_VERSION;
    public static String VENDOR_NAME;
    public static String SPECIFICATION_VERSION;

    private Penrose penrose;

    private ServiceConfigManager serviceConfigManager;
    private ServiceManager serviceManager;

    static {
        try {
            Package pkg = PenroseServer.class.getPackage();

            String value = pkg.getImplementationTitle();
            if (value != null) PRODUCT_NAME = value;

            value = pkg.getImplementationVersion();
            if (value != null) PRODUCT_VERSION = value;

            value = pkg.getImplementationVendor();
            if (value != null) VENDOR_NAME = value;

            value = pkg.getSpecificationVersion();
            if (value != null) SPECIFICATION_VERSION = value;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public PenroseServer() throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose();

        init();
    }

    public PenroseServer(String home) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(home);

        init();
    }

    public PenroseServer(File home) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(home);

        init();
    }

    public PenroseServer(String home, PenroseConfig penroseConfig) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(home, penroseConfig);

        init();
    }

    public PenroseServer(File home, PenroseConfig penroseConfig) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(home, penroseConfig);

        init();
    }

    public PenroseServer(PenroseConfig penroseConfig) throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);

        init();
    }

    public void init() throws Exception {
        
        File home = penrose.getHome();
        File servicesDir = new File(home, "services");

        serviceConfigManager = new ServiceConfigManager(servicesDir);
        serviceManager = new ServiceManager(this, serviceConfigManager);
    }

    public void start() throws Exception {

        log.debug("Starting "+PRODUCT_NAME+"...");

        penrose.start();

        log.debug("----------------------------------------------------------------------------------");

        for (String serviceName : serviceConfigManager.getAvailableServiceNames()) {

            try {
                serviceManager.loadServiceConfig(serviceName);
                serviceManager.startService(serviceName);

            } catch (Exception e) {
                errorLog.error(e.getMessage(), e);
            }
        }

        log.fatal(PRODUCT_NAME+" is ready.");
    }

    public void stop() throws Exception {

        log.debug("----------------------------------------------------------------------------------");
        log.debug("Stopping "+PRODUCT_NAME+"...");

        List<String> serviceNames = new ArrayList<String>();

        for (String serviceName : serviceManager.getServiceNames()) {
            serviceNames.add(0, serviceName);
        }

        for (String serviceName : serviceNames) {

            try {
                serviceManager.stopService(serviceName);
                serviceManager.unloadService(serviceName);

            } catch (Exception e) {
                errorLog.error(e.getMessage(), e);
            }
        }

        penrose.stop();

        log.fatal(PRODUCT_NAME+" has been shutdown.");
    }

    public String getStatus() {
        return penrose.getStatus();
    }

    public File getHome() {
        return penrose.getHome();
    }

    public void listAllThreads() {
        // Find the root thread group
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        log.debug("ThreadGroup: "+root.getName());

        while (root.getParent() != null) {
            root = root.getParent();
            log.debug("ThreadGroup: "+root.getName());
        }

        // Visit each thread group
        visit(root, 0);
    }

    private void visit(ThreadGroup group, int level) {
        //logger.debug(group.toString());

        // Get threads in 'group'
        int numThreads = group.activeCount();
        Thread[] threads = new Thread[numThreads * 2];
        numThreads = group.enumerate(threads, false);

        // Enumerate each thread in 'group'
        for (int i = 0; i < numThreads; i++) {
            // Get thread
            Thread thread = threads[i];
            StringBuilder sb = new StringBuilder();
            for (int j=0; j<level; j++) sb.append("  ");
            sb.append(thread.toString());
            log.debug(sb.toString());
        }

        // Get thread subgroups of 'group'
        int numGroups = group.activeGroupCount();
        ThreadGroup[] groups = new ThreadGroup[numGroups * 2];
        numGroups = group.enumerate(groups, false);

        // Recursively visit each subgroup
        for (int i = 0; i < numGroups; i++) {
            visit(groups[i], level + 1);
        }
    }

    public PenroseConfig getPenroseConfig() {
        return penrose.getPenroseConfig();
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public ServiceManager getServiceManager() {
        return serviceManager;
    }

    public void setServiceManager(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    public void reload() throws Exception {

        penrose.reload();

        serviceManager.clear();
        //serviceManager.load(penroseConfig.getServiceConfigManager());
    }

    public static void main(String[] args) throws Exception {

        try {
            Collection parameters = Arrays.asList(args);

            if (parameters.contains("-?") || parameters.contains("--help")) {
                System.out.println("Usage: org.safehaus.penrose.server.PenroseServer [OPTION]...");
                System.out.println();
                System.out.println("  -?, --help     display this help and exit");
                System.out.println("  -d             run in debug mode");
                System.out.println("  -v             run in verbose mode");
                System.out.println("      --version  output version information and exit");
                System.exit(0);
            }

            if (parameters.contains("--version")) {
                System.out.println(PRODUCT_NAME+" "+PRODUCT_VERSION);
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
                        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss.SSS}] %m%n"));
                        logger.addAppender(appender);
                    }
                }
            }

            log.warn("Starting "+PRODUCT_NAME+" "+PRODUCT_VERSION+".");

            String javaVersion = System.getProperty("java.version");
            log.info("Java version: "+javaVersion);

            String javaVendor = System.getProperty("java.vendor");
            log.info("Java vendor: "+javaVendor);
            
            String javaHome = System.getProperty("java.home");
            log.info("Java home: "+javaHome);

            String userDir = System.getProperty("user.dir");
            log.info("Current directory: "+userDir);

            log.info(PRODUCT_NAME+" home: "+home);

            final PenroseServer penroseServer = new PenroseServer(home);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        penroseServer.stop();
                    } catch (Exception e) {
                        errorLog.error(e.getMessage(), e);
                    }
                }
            });

            penroseServer.start();

        } catch (Exception e) {
            log.error(PRODUCT_NAME+" failed to start: "+e.getMessage(), e);
            System.exit(1);
        }
    }

    public ServiceConfigManager getServiceConfigs() {
        return serviceConfigManager;
    }

    public String getProductVendor() {
        return VENDOR_NAME;
    }

    public String getProductName() {
        return PRODUCT_NAME;
    }

    public String getProductVersion() {
        return PRODUCT_VERSION;
    }
}
