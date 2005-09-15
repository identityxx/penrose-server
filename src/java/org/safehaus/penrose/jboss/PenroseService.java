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
