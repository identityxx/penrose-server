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

import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.log.log4j.AppenderConfig;
import org.safehaus.penrose.config.PenroseConfigReader;
import org.safehaus.penrose.config.PenroseConfigWriter;
import org.safehaus.penrose.log.log4j.Log4jConfig;
import org.safehaus.penrose.log.log4j.Log4jConfigReader;
import org.safehaus.penrose.log.log4j.LoggerConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionContext;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.statistic.StatisticManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class Penrose {

    public Logger log = LoggerFactory.getLogger(getClass());

    public static Logger accessLog = org.safehaus.penrose.log.Access.log;
    public static Logger errorLog = org.safehaus.penrose.log.Error.log;

    public static String PRODUCT_NAME;
    public static String PRODUCT_VERSION;
    public static String VENDOR_NAME;
    public static String SPECIFICATION_VERSION;

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    private File           home;

    private PenroseConfig  penroseConfig;
    private PenroseContext penroseContext;
    private SessionContext sessionContext;

    private StatisticManager statisticManager;

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

    protected Penrose() throws Exception {
        penroseConfig = new DefaultPenroseConfig();

        init();
    }

    protected Penrose(String home) throws Exception {
        this.home = new File(home);
        penroseConfig = loadConfig(home);

        init();
    }

    protected Penrose(File home) throws Exception {
        this.home = home;
        penroseConfig = loadConfig(home);

        init();
    }

    protected Penrose(String home, PenroseConfig penroseConfig) throws Exception {
        this.home = new File(home);
        this.penroseConfig = penroseConfig;

        init();
    }

    protected Penrose(File home, PenroseConfig penroseConfig) throws Exception {
        this.home = home;
        this.penroseConfig = penroseConfig;

        init();
    }

    protected Penrose(PenroseConfig penroseConfig) throws Exception {
        this.penroseConfig = penroseConfig;

        init();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Load Penrose Configurations
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PenroseConfig loadConfig() throws Exception {
        return loadConfig((File)null);
    }

    public PenroseConfig loadConfig(String dir) throws Exception {
        return loadConfig(new File(dir));
    }

    public PenroseConfig loadConfig(File dir) throws Exception {

        File conf = new File(dir, "conf");
        File serverXml = new File(conf, "server.xml");

        PenroseConfigReader reader = new PenroseConfigReader();
        return reader.read(serverXml);
    }

    public void store() throws Exception {

        File conf = new File(home, "conf");
        File serverXml = new File(conf, "server.xml");

        PenroseConfigWriter writer = new PenroseConfigWriter();
        writer.write(serverXml, penroseConfig);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Initialize Penrose components
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void init() throws Exception {

        File log4jXml = new File(home, "conf"+File.separator+"log4j.xml");

        if (log4jXml.exists()) {
            Log4jConfigReader configReader = new Log4jConfigReader();
            Log4jConfig config = configReader.read(log4jXml);

            log.debug("Appenders:");
            for (AppenderConfig appenderConfig : config.getAppenderConfigs()) {
                log.debug(" - " + appenderConfig.getName());
            }

            log.debug("Loggers:");
            for (LoggerConfig loggerConfig : config.getLoggerConfigs()) {
                log.debug(" - " + loggerConfig.getName() + ": " + loggerConfig.getLevel() + " " + loggerConfig.getAppenderNames());
            }
        }

        penroseContext = new PenroseContext(home);
        sessionContext = new SessionContext(this);

        penroseContext.setSessionContext(sessionContext);
        penroseContext.init(penroseConfig);

        sessionContext.setPenroseConfig(penroseConfig);
        sessionContext.setPenroseContext(penroseContext);
        sessionContext.init();

        sessionContext.load();

        statisticManager = new StatisticManager();
    }

    public void clear() throws Exception {

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        partitionManager.clear();

        penroseContext.clear();
    }

    public void reload() throws Exception {
        clear();
        loadConfig();
        init();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Start Penrose
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void start() throws Exception {

        if (!STOPPED.equals(status)) return;

        status = STARTING;

        penroseContext.start();

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        partitionManager.startPartitions();

        sessionContext.start();

        status = STARTED;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Stop Penrose
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void stop() throws Exception {

        if (!STARTED.equals(status)) return;

        status = STOPPING;

        sessionContext.stop();

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        partitionManager.stopPartitions();

        penroseContext.stop();

        status = STOPPED;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Schemas
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SchemaManager getSchemaManager() {
        return penroseContext.getSchemaManager();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Partitions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PartitionManager getPartitionManager() {
        return penroseContext.getPartitionManager();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sessions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Session createSession() throws Exception {
        SessionManager sessionManager = sessionContext.getSessionManager();
        return sessionManager.createSession();
    }

    public Session createSession(String sessionId) throws Exception {
        SessionManager sessionManager = sessionContext.getSessionManager();
        return sessionManager.createSession(sessionId);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Setters & Getters
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public String getStatus() {
        return status;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }

    public File getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = new File(home);
        penroseContext.setHome(this.home);
    }

    public StatisticManager getStatisticManager() {
        return statisticManager;
    }
}
