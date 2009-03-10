package org.safehaus.penrose.log;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.log.log4j.*;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class LogManager {

    Logger log = Logger.getLogger(LogManager.class);

    File homeDir;
    File confDir;
    File log4jXml;

    Log4jConfig log4jConfig;
    
    public LogManager(File homeDir) {
        this.homeDir = homeDir;
        this.confDir = new File(homeDir, "conf");
        this.log4jXml = new File(confDir, "log4j.xml");
    }

    public void load() throws Exception {
        Log4jConfigReader reader = new Log4jConfigReader();
        log4jConfig = reader.read(log4jXml);
    }

    public void store() throws Exception {
        Log4jConfigWriter writer = new Log4jConfigWriter();
        writer.write(log4jXml, log4jConfig);

        DOMConfigurator.configure(log4jXml.getAbsolutePath());
    }

    public Collection<AppenderConfig> getAppenderConfigs() {
        return log4jConfig.getAppenderConfigs();
    }

    public Collection<String> getAppenderNames() {
        return log4jConfig.getAppenderConfigNames();
    }

    public AppenderConfig getAppenderConfig(String appenderName) {
        return log4jConfig.getAppenderConfig(appenderName);
    }

    public void addAppender(AppenderConfig appenderConfig) {
        log4jConfig.addAppenderConfig(appenderConfig);
    }

    public void updateAppender(String appenderName, AppenderConfig appenderConfig) throws Exception {
        log4jConfig.updateAppenderConfig(appenderName, appenderConfig);
    }

    public void removeAppender(String appenderName) {
        log4jConfig.removeAppenderConfig(appenderName);
    }

    public Collection<LoggerConfig> getLoggerConfigs() {
        return log4jConfig.getLoggerConfigs();
    }

    public Collection<String> getLoggerNames() {
        return log4jConfig.getLoggerConfigNames();
    }

    public LoggerConfig getLoggerConfig(String loggerName) {
        return log4jConfig.getLoggerConfig(loggerName);
    }

    public void addLogger(LoggerConfig loggerConfig) {
        log4jConfig.addLoggerConfig(loggerConfig);
    }

    public void updateLogger(String loggerName, LoggerConfig loggerConfig) throws Exception {
        log4jConfig.updateLoggerConfig(loggerName, loggerConfig);
    }

    public void removeLogger(String loggerName) {
        log4jConfig.removeLoggerConfig(loggerName);
    }

    public RootLoggerConfig getRootLoggerConfig() throws Exception {
        return log4jConfig.getRootLoggerConfig();
    }

    public void updateRootLogger(RootLoggerConfig rootLoggerConfig) throws Exception {
        log4jConfig.setRootLoggerConfig(rootLoggerConfig);
    }
}
