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

    public Collection<String> getAppenderConfigNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(logManager.getAppenderConfigNames());
        return list;
    }

    public AppenderConfig getAppenderConfig(String appenderName) throws Exception {
        return logManager.getAppenderConfig(appenderName);
    }

    public void addAppenderConfig(AppenderConfig appenderConfig) throws Exception {
        logManager.addAppenderConfig(appenderConfig);
    }

    public void updateAppenderConfig(String appenderName, AppenderConfig appenderConfig) throws Exception {
        logManager.updateAppenderConfig(appenderName, appenderConfig);
    }

    public AppenderConfig removeAppenderConfig(String appenderName) throws Exception {
        return logManager.removeAppenderConfig(appenderName);
    }

    public Collection<LoggerConfig> getLoggerConfigs() throws Exception {
        Collection<LoggerConfig> list = new ArrayList<LoggerConfig>();
        list.addAll(logManager.getLoggerConfigs());
        return list;
    }

    public Collection<String> getLoggerConfigNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(logManager.getLoggerConfigNames());
        return list;
    }

    public LoggerConfig getLoggerConfig(String loggerName) throws Exception {
        return logManager.getLoggerConfig(loggerName);
    }

    public void addLoggerConfig(LoggerConfig loggerConfig) throws Exception {
        logManager.addLoggerConfig(loggerConfig);
    }

    public void updateLoggerConfig(String loggerName, LoggerConfig loggerConfig) throws Exception {
        logManager.updateLoggerConfig(loggerName, loggerConfig);
    }

    public LoggerConfig removeLoggerConfig(String loggerName) throws Exception {
        return logManager.removeLoggerConfig(loggerName);
    }

    public RootLoggerConfig getRootLoggerConfig() throws Exception {
        return logManager.getRootLoggerConfig();
    }

    public void setRootLoggerConfig(RootLoggerConfig rootLoggerConfig) throws Exception {
        logManager.setRootLoggerConfig(rootLoggerConfig);
    }

    public void store() throws Exception {
        logManager.store();
    }
}
