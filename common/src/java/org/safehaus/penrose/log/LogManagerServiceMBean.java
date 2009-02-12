package org.safehaus.penrose.log;

import org.safehaus.penrose.log.log4j.AppenderConfig;
import org.safehaus.penrose.log.log4j.LoggerConfig;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface LogManagerServiceMBean {

    public Collection<AppenderConfig> getAppenderConfigs() throws Exception;
    public Collection<String> getAppenderConfigNames() throws Exception;
    public AppenderConfig getAppenderConfig(String appenderName) throws Exception;
    public void addAppenderConfig(AppenderConfig appenderConfig) throws Exception;
    public void updateAppenderConfig(String appenderName, AppenderConfig appenderConfig) throws Exception;
    public AppenderConfig removeAppenderConfig(String appenderName) throws Exception;

    public Collection<LoggerConfig> getLoggerConfigs() throws Exception;
    public Collection<String> getLoggerConfigNames() throws Exception;
    public LoggerConfig getLoggerConfig(String loggerName) throws Exception;
    public void addLoggerConfig(LoggerConfig loggerConfig) throws Exception;
    public void updateLoggerConfig(String loggerName, LoggerConfig loggerConfig) throws Exception;
    public LoggerConfig removeLoggerConfig(String loggerName) throws Exception;
}
