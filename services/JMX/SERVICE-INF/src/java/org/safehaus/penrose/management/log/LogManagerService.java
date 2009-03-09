package org.safehaus.penrose.management.log;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.log.LogManager;
import org.safehaus.penrose.log.LogManagerClient;
import org.safehaus.penrose.log.LogManagerServiceMBean;
import org.safehaus.penrose.log.log4j.AppenderConfig;
import org.safehaus.penrose.log.log4j.LoggerConfig;
import org.safehaus.penrose.log.log4j.RootLoggerConfig;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class LogManagerService extends BaseService implements LogManagerServiceMBean {

    LogManager logManager;

    public LogManagerService(PenroseJMXService jmxService, LogManager logManager) {
        this.jmxService = jmxService;
        this.logManager = logManager;
    }

    public Object getObject() {
        return logManager;
    }

    public String getObjectName() {
        return LogManagerClient.getStringObjectName();
    }

    public Collection<AppenderConfig> getAppenderConfigs() throws Exception {
        Collection<AppenderConfig> list = new ArrayList<AppenderConfig>();
        list.addAll(logManager.getAppenderConfigs());
        return list;
    }

    public Collection<String> getAppenderNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(logManager.getAppenderNames());
        return list;
    }

    public AppenderConfig getAppenderConfig(String appenderName) throws Exception {
        return logManager.getAppenderConfig(appenderName);
    }

    public void addAppender(AppenderConfig appenderConfig) throws Exception {
        logManager.addAppender(appenderConfig);
    }

    public void updateAppender(String appenderName, AppenderConfig appenderConfig) throws Exception {
        logManager.updateAppender(appenderName, appenderConfig);
    }

    public void removeAppender(String appenderName) throws Exception {
        logManager.removeAppender(appenderName);
    }

    public Collection<LoggerConfig> getLoggerConfigs() throws Exception {
        Collection<LoggerConfig> list = new ArrayList<LoggerConfig>();
        list.addAll(logManager.getLoggerConfigs());
        return list;
    }

    public Collection<String> getLoggerNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(logManager.getLoggerNames());
        return list;
    }

    public LoggerConfig getLoggerConfig(String loggerName) throws Exception {
        return logManager.getLoggerConfig(loggerName);
    }

    public void addLogger(LoggerConfig loggerConfig) throws Exception {
        logManager.addLogger(loggerConfig);
    }

    public void updateLogger(String loggerName, LoggerConfig loggerConfig) throws Exception {
        logManager.updateLogger(loggerName, loggerConfig);
    }

    public void removeLogger(String loggerName) throws Exception {
        logManager.removeLogger(loggerName);
    }

    public RootLoggerConfig getRootLoggerConfig() throws Exception {
        return logManager.getRootLoggerConfig();
    }

    public void updateRootLogger(RootLoggerConfig rootLoggerConfig) throws Exception {
        logManager.updateRootLogger(rootLoggerConfig);
    }

    public void store() throws Exception {
        logManager.store();
    }
}
