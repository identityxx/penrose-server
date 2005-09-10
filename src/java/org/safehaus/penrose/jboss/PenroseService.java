/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.jboss;

import org.safehaus.penrose.PenroseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * @author Endi S. Dewata
 */
public class PenroseService implements PenroseServiceMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    private String home;
    private int port;

    public void create() throws Exception {
        log.debug("Creating Penrose Service");
    }

    public void start() throws Exception {
        log.debug("Starting Penrose Service...");

        PenroseServer server = new PenroseServer(home);
        server.run();

        log.debug("Penrose Service started.");
    }

    public void stop() {
        log.debug("Stopping Penrose Service");
    }

    public void destroy() {
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }
}
