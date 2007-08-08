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
import java.util.Collection;

import org.safehaus.penrose.config.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.SessionContext;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.connector.ConnectorContext;
import org.safehaus.penrose.log4j.Log4jConfigReader;
import org.safehaus.penrose.log4j.Log4jConfig;
import org.safehaus.penrose.log4j.AppenderConfig;
import org.safehaus.penrose.log4j.LoggerConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.handler.HandlerConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class Penrose {

    public Logger log = LoggerFactory.getLogger(getClass());
    public Logger errorLog = org.safehaus.penrose.log.Error.log;
    public boolean debug = log.isDebugEnabled();

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

    private File               home;

    private PenroseConfig      penroseConfig;
    private PenroseContext     penroseContext;
    private ConnectorContext   connectorContext;
    private SessionContext     sessionContext;

    private PartitionConfigs   partitionConfigs;
    private PartitionValidator partitionValidator;
    private Partitions         partitions;

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
        File path = new File(dir, "conf"+File.separator+"server.xml");
        PenroseConfigReader reader = new PenroseConfigReader(path);
        return reader.read();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Initialize Penrose components
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void init() throws Exception {

        File log4jXml = new File(home, "conf"+File.separator+"log4j.xml");

        if (log4jXml.exists()) {
            Log4jConfigReader configReader = new Log4jConfigReader(log4jXml);
            Log4jConfig config = configReader.read();

            log.debug("Appenders:");
            for (AppenderConfig appenderConfig : config.getAppenderConfigs()) {
                log.debug(" - " + appenderConfig.getName());
            }

            log.debug("Loggers:");
            for (LoggerConfig loggerConfig : config.getLoggerConfigs()) {
                log.debug(" - " + loggerConfig.getName() + ": " + loggerConfig.getLevel() + " " + loggerConfig.getAppenders());
            }
        }

        penroseContext = new PenroseContext(home);
        connectorContext = new ConnectorContext();
        sessionContext = new SessionContext();

        partitionConfigs = new PartitionConfigs();

        partitions = new Partitions();

        partitionValidator = new PartitionValidator();
        partitionValidator.setPenroseConfig(penroseConfig);
        partitionValidator.setPenroseContext(penroseContext);

        penroseContext.setSessionContext(sessionContext);
        penroseContext.setPartitionConfigs(partitionConfigs);
        penroseContext.setPartitions(partitions);
        penroseContext.init(penroseConfig);

        connectorContext.setPenroseConfig(penroseConfig);
        connectorContext.setPenroseContext(penroseContext);
        connectorContext.init();

        sessionContext.setPenroseConfig(penroseConfig);
        sessionContext.setPenroseContext(penroseContext);
        sessionContext.init();

        sessionContext.load();
    }

    public void clear() throws Exception {
        partitionConfigs.clear();
        partitions.clear();
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

        if (debug) log.debug("----------------------------------------------------------------------------------");
        log.debug("Loading DEFAULT partition.");

        File conf = new File(home, "conf");

        PartitionReader partitionReader = partitionConfigs.getPartitionReader();

        PartitionConfig partitionConfig = new PartitionConfig("DEFAULT");

        for (HandlerConfig handlerConfig : penroseConfig.getHandlerConfigs()) {
            partitionConfig.addHandlerConfig(handlerConfig);
        }

        for (EngineConfig engineConfig : penroseConfig.getEngineConfigs()) {
            partitionConfig.addEngineConfig(engineConfig);
        }

        partitionReader.read(conf, partitionConfig.getConnectionConfigs());
        partitionReader.read(conf, partitionConfig.getSourceConfigs());
        partitionReader.read(conf, partitionConfig.getDirectoryConfigs());
        partitionReader.read(conf, partitionConfig.getModuleConfigs());

        partitionConfigs.addPartitionConfig(partitionConfig);

        Collection<PartitionValidationResult> results = partitionValidator.validate(partitionConfig);

        for (PartitionValidationResult result : results) {
            if (result.getType().equals(PartitionValidationResult.ERROR)) {
                errorLog.error("ERROR: " + result.getMessage() + " [" + result.getSource() + "]");
            } else {
                errorLog.warn("WARNING: " + result.getMessage() + " [" + result.getSource() + "]");
            }
        }

        log.debug("Starting DEFAULT partition.");

        PartitionContext partitionContext = new PartitionContext();
        partitionContext.setPenroseConfig(penroseConfig);
        partitionContext.setPenroseContext(penroseContext);

        Partition partition = new Partition();
        partition.init(partitionConfig, partitionContext);
        partitions.addPartition(partition);

        File partitionsDir = new File(home, "partitions");
        if (!partitionsDir.isDirectory()) return;

        for (File partitionDir : partitionsDir.listFiles()) {
            if (!partitionDir.isDirectory()) continue;

            if (debug) log.debug("----------------------------------------------------------------------------------");

            partitionConfig = partitionConfigs.load(partitionDir);
            String name = partitionConfig.getName();

            if (!partitionConfig.isEnabled()) {
                log.debug(name+" partition is disabled.");
                continue;
            }

            results = partitionValidator.validate(partitionConfig);

            for (PartitionValidationResult result : results) {
                if (result.getType().equals(PartitionValidationResult.ERROR)) {
                    errorLog.error("ERROR: " + result.getMessage() + " [" + result.getSource() + "]");
                } else {
                    errorLog.warn("WARNING: " + result.getMessage() + " [" + result.getSource() + "]");
                }
            }

            log.debug("Starting "+name+" partition.");

            partitionContext = new PartitionContext();
            partitionContext.setPath(partitionDir);
            partitionContext.setPenroseConfig(penroseConfig);
            partitionContext.setPenroseContext(penroseContext);

            partition = new Partition();
            partition.init(partitionConfig, partitionContext);
            partitions.addPartition(partition);
        }

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

        partitions.stop();

        penroseContext.stop();

        status = STOPPED;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Penrose Sessions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Session newSession() throws Exception {
        SessionManager sessionManager = sessionContext.getSessionManager();
        return sessionManager.newSession();
    }

    public Session createSession(Object sessionId) throws Exception {
        SessionManager sessionManager = sessionContext.getSessionManager();
        return sessionManager.createSession(sessionId);
    }

    public Session getSession(String sessionId) throws Exception {
        SessionManager sessionManager = sessionContext.getSessionManager();
        return sessionManager.getSession(sessionId);
    }

    public Session removeSession(String sessionId) throws Exception {
        SessionManager sessionManager = sessionContext.getSessionManager();
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

    public ConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public void setConnectorContext(ConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
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

    public PartitionConfigs getPartitionConfigs() {
        return partitionConfigs;
    }

    public void setPartitionConfigs(PartitionConfigs partitionConfigs) {
        this.partitionConfigs = partitionConfigs;
    }

    public Partitions getPartitions() {
        return partitions;
    }

    public void setPartitions(Partitions partitions) {
        this.partitions = partitions;
    }
}
