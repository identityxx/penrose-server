/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
package org.safehaus.penrose;

import java.util.*;
import java.io.File;

import org.apache.log4j.*;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.ldap.PenroseLDAPService;
import sun.misc.Signal;
import sun.misc.SignalHandler;


/**
 * @author Endi S. Dewata
 */
public class PenroseServer implements SignalHandler {

    public static Logger log = Logger.getLogger(PenroseServer.class);

    String home;
    PenroseConfig penroseConfig;

    Penrose penrose;

    PenroseJMXService jmxService;
    PenroseLDAPService ldapService;

    public PenroseServer(String homeDirectory) throws Exception {
        this.home = homeDirectory;
    }

    public void start() throws Exception {
        startPenroseService();
        startJmxService();
        startLdapService();
    }

    public void stop() throws Exception {
        stopLdapService();
        stopJmxService();
        stopPenroseService();
    }

    public void startPenroseService() throws Exception {
        penrose = new Penrose();
        penrose.setHome(home);
        penrose.start();

        penroseConfig = penrose.getConfig();
    }

    public void stopPenroseService() throws Exception {
        penrose.stop();
    }

    public void startLdapService() throws Exception {
        ldapService = new PenroseLDAPService();
        ldapService.setHomeDirectory(home);
        ldapService.setPenrose(penrose);
        ldapService.start();
    }

    public void stopLdapService() throws Exception {
        ldapService.stop();
    }

    public void startJmxService() throws Exception {
        jmxService = new PenroseJMXService();
        jmxService.setPenrose(penrose);
        jmxService.start();
    }

    public void stopJmxService() throws Exception {
        jmxService.stop();
    }

    /**
     * Ctrl-C (Interrupt Signal) handler
     *
     * The obvious drawback with this is that it relies on undocumented classes from
     * Sun. There are other solutions, including one given at
     * http://www.naturalbridge.com/useful/index.html and another at
     * http://interstice.com/~kevinh/projects/javasignals/ but both of these use
     * native code.
     */
    public void initSignalHandler() {
        Signal.handle(new Signal("INT"), this);
    }

    public void handle(Signal sig) {
        //code to be executed goes here
        log.info("Interrupt Signal (Ctrl-C) caught! Initiating shutdown...");
        listAllThreads();
        
        try {
            stop();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public void listAllThreads() {
        // Find the root thread group
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        log.debug(".. ThreadGroup: "+root.getName());

        while (root.getParent() != null) {
            root = root.getParent();
            log.debug(".. ThreadGroup: "+root.getName());
        }

        // Visit each thread group
        visit(root, 0);
    }

    /**
     * This method recursively visits all thread groups under `group'.
     */
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
            StringBuffer sb = new StringBuffer();
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

    public static void main( String[] args ) throws Exception {

        try {
            Collection parameters = Arrays.asList(args);

            if (parameters.contains("-?") || parameters.contains("--help")) {
                System.out.println("Usage: org.safehaus.penrose.PenroseServer [OPTION]...");
                System.out.println("  -?, --help     display this help and exit");
                System.out.println("  -d             run in debug mode");
                System.out.println("  -v             run in verbose mode");
                System.out.println("      --version  output version information and exit");
                System.exit(0);
            }

            if (parameters.contains("--version")) {
                System.out.println(Penrose.PRODUCT_NAME);
                System.out.println(Penrose.PRODUCT_COPYRIGHT);
                System.exit(0);
            }

            String homeDirectory = System.getProperty("penrose.home");

            Logger rootLogger = Logger.getRootLogger();
            rootLogger.setLevel(Level.toLevel("OFF"));

            Logger logger = Logger.getLogger("org.safehaus.penrose");
            File log4jProperties = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.properties");

            if (log4jProperties.exists()) {
                PropertyConfigurator.configure(log4jProperties.getAbsolutePath());

            } else if (parameters.contains("-d")) {
                logger.setLevel(Level.toLevel("DEBUG"));
                ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
                BasicConfigurator.configure(appender);

            } else if (parameters.contains("-v")) {
                logger.setLevel(Level.toLevel("INFO"));
                ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
                BasicConfigurator.configure(appender);

            } else {
                logger.setLevel(Level.toLevel("WARN"));
                ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
                BasicConfigurator.configure(appender);
            }

            log.warn("Starting "+Penrose.PRODUCT_NAME+".");

            PenroseServer server = new PenroseServer(homeDirectory);
            server.start();

            log.warn("Server is ready.");

        } catch (Exception e) {
            String name = e.getClass().getName();
            name = name.substring(name.lastIndexOf(".")+1);
            log.debug(name, e);
            log.error("Server failed to start: "+name+": "+e.getMessage());
            System.exit(1);
        }
    }
}
