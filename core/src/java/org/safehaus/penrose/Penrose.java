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
package org.safehaus.penrose;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.safehaus.penrose.config.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.naming.PenroseContext;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class Penrose {

    Logger log = LoggerFactory.getLogger(getClass());

    public static String PRODUCT_NAME          = "Penrose";
    public static String PRODUCT_VERSION       = "1.2";
    public static String VENDOR_NAME           = "Identyx";
    public static String PRODUCT_COPYRIGHT     = "Copyright (c) 2000-2007, Identyx Corporation.";
    public static String SPECIFICATION_VERSION = "1.2";

    public final static DateFormat DATE_FORMAT   = new SimpleDateFormat("MM/dd/yyyy");
    public final static String RELEASE_DATE      = "03/01/2007";

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    private PenroseConfig      penroseConfig;
    private PenroseContext     penroseContext;

    private String status = STOPPED;

    static {
        try {
            Package pkg = Penrose.class.getPackage();

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

    protected Penrose(PenroseConfig penroseConfig) throws Exception {
        this.penroseConfig = penroseConfig;
        init();
        load();
    }

    protected Penrose(String home) throws Exception {

        penroseConfig = new PenroseConfig();
        penroseConfig.setHome(home);
        loadConfig();

        init();
        load();
    }

    protected Penrose() throws Exception {
        penroseConfig = new PenroseConfig();
        loadConfig();

        init();
        load();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Load Penrose Configurations
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void loadConfig() throws Exception {

        String home = penroseConfig.getHome();

        penroseConfig.clear();

        PenroseConfigReader reader = new PenroseConfigReader((home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml");
        reader.read(penroseConfig);
        penroseConfig.setHome(home);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Initialize Penrose components
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void init() throws Exception {
        penroseContext = new PenroseContext();
        penroseContext.init(penroseConfig);
    }

    public void load() throws Exception {
        penroseContext.load();
    }

    public void clear() throws Exception {
        penroseContext.clear();
    }

    public void reload() throws Exception {
        clear();
        loadConfig();
        init();
        load();
    }

    public void store() throws Exception {
        penroseContext.store();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Start Penrose
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void start() throws Exception {

        if (status != STOPPED) return;

        status = STARTING;

        penroseContext.start();

        status = STARTED;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Stop Penrose
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void stop() throws Exception {

        if (status != STARTED) return;

        status = STOPPING;

        penroseContext.stop();

        status = STOPPED;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Penrose Sessions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Session newSession() throws Exception {
        SessionManager sessionManager = penroseContext.getSessionManager();
        return sessionManager.newSession();
    }

    public Session createSession(Object sessionId) throws Exception {
        SessionManager sessionManager = penroseContext.getSessionManager();
        return sessionManager.createSession(sessionId);
    }

    public Session getSession(String sessionId) throws Exception {
        SessionManager sessionManager = penroseContext.getSessionManager();
        return sessionManager.getSession(sessionId);
    }

    public Session removeSession(String sessionId) throws Exception {
        SessionManager sessionManager = penroseContext.getSessionManager();
        return sessionManager.removeSession(sessionId);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Setters & Getters
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public String getStatus() {
        return status;
    }
}
