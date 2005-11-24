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

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.config.ServerConfig;
import org.safehaus.penrose.ldap.PenroseLDAPService;
import sun.misc.Signal;
import sun.misc.SignalHandler;


/**
 * @author Endi S. Dewata
 */
public class PenroseServer implements SignalHandler {

    public static Logger log = Logger.getLogger(PenroseServer.class);

    Properties env;

    String homeDirectory;
    ServerConfig serverConfig;

    Penrose penrose;

    PenroseJMXService jmxService;
    PenroseLDAPService ldapService;

    public PenroseServer(String homeDirectory) throws Exception {
        this.homeDirectory = homeDirectory;
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
        penrose.setHomeDirectory(homeDirectory);
        penrose.start();

        serverConfig = penrose.getServerConfig();
    }

    public void stopPenroseService() throws Exception {
        penrose.stop();
    }

    public void startLdapService() throws Exception {
        ldapService = new PenroseLDAPService();
        ldapService.setHomeDirectory(homeDirectory);
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
        penrose.stop();
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
            log.info("Starting Penrose Server 0.9.8.");

            String home = System.getProperty("penrose.home");
            //log.debug("Home: "+home);

            File log4jProperties = new File((home == null ? "" : home+File.separator)+"conf"+File.separator+"log4j.properties");
            if (log4jProperties.exists()) {
                log.debug("Loading "+log4jProperties.getPath());
                PropertyConfigurator.configure(log4jProperties.getAbsolutePath());
            }

            PenroseServer server = new PenroseServer(home);
            server.start();

            log.info("Penrose Server is ready.");

        } catch (Exception e) {
            String name = e.getClass().getName();
            name = name.substring(name.lastIndexOf(".")+1);
            log.debug(name, e);
            log.error("Penrose Server failed to start: "+name+": "+e.getMessage());
            System.exit(1);
        }
    }
}
