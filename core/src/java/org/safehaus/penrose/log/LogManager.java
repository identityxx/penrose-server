package org.safehaus.penrose.log;

import org.apache.log4j.Logger;
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

    Log4jConfig log4jConfig;
    
    Map loggers = new TreeMap();

    public LogManager(File homeDir) {
        this.homeDir = homeDir;
        this.confDir = new File(homeDir, "conf");
    }

    public void load() throws Exception {
        Log4jConfigReader reader = new Log4jConfigReader();
        log4jConfig = reader.read(new File(confDir, "log4j.xml"));
    }

    public void store() throws Exception {
        Log4jConfigWriter writer = new Log4jConfigWriter(new File(confDir, "/conf/log4j.xml"));
        writer.write(log4jConfig);
    }

    public Collection<AppenderConfig> getAppenderConfigs() {
        return log4jConfig.getAppenderConfigs();
    }

    public Collection<String> getAppenderConfigNames() {
        return log4jConfig.getAppenderConfigNames();
    }

    public AppenderConfig getAppenderConfig(String appenderName) {
        return log4jConfig.getAppenderConfig(appenderName);
    }

    public void addAppenderConfig(AppenderConfig appenderConfig) {
        log4jConfig.addAppenderConfig(appenderConfig);
    }

    public void updateAppenderConfig(String appenderName, AppenderConfig appenderConfig) {
        removeAppenderConfig(appenderName);
        addAppenderConfig(appenderConfig);
    }

    public AppenderConfig removeAppenderConfig(String appenderName) {
        return log4jConfig.removeAppenderConfig(appenderName);
    }

    public Collection<LoggerConfig> getLoggerConfigs() {
        return log4jConfig.getLoggerConfigs();
    }

    public Collection<String> getLoggerConfigNames() {
        return log4jConfig.getLoggerConfigNames();
    }

    public LoggerConfig getLoggerConfig(String loggerName) {
        return log4jConfig.getLoggerConfig(loggerName);
    }

    public void addLoggerConfig(LoggerConfig loggerConfig) {
        log4jConfig.addLoggerConfig(loggerConfig);
    }

    public void updateLoggerConfig(String loggerName, LoggerConfig loggerConfig) {
        removeLoggerConfig(loggerName);
        addLoggerConfig(loggerConfig);
    }

    public LoggerConfig removeLoggerConfig(String loggerName) {
        return log4jConfig.removeLoggerConfig(loggerName);
    }

    public void addLogger(String name) {
        StringTokenizer st = new StringTokenizer(name, ".");
        Map map = loggers;

        //log.debug("Adding logger:");
        while (st.hasMoreTokens()) {
            String rname = st.nextToken();
            //log.debug(" - "+rname);

            Map m = (Map)map.get(rname);
            if (m == null) {
                m = new TreeMap();
                map.put(rname, m);
            }

            map = m;
        }
    }

    public void removeLogger(String name) {
        StringTokenizer st = new StringTokenizer(name, ".");
        Map map = loggers;

        //log.debug("Adding logger:");
        while (st.hasMoreTokens()) {
            String rname = st.nextToken();
            //log.debug(" - "+rname);

            Map m = (Map)map.get(rname);
            if (m == null) {
                m = new TreeMap();
                map.put(rname, m);
            }

            if (!st.hasMoreTokens()) {
                map.remove(rname);
                return;
            }
            
            map = m;
        }
    }

    public Collection getLoggers() {
        return getLoggers(null);
    }

    public Collection getLoggers(String name) {
        if (name == null) {
            return loggers.keySet();
        }

        StringTokenizer st = new StringTokenizer(name, ".");
        Map map = loggers;

        //log.debug("Getting logger:");
        while (st.hasMoreTokens()) {
            String rname = st.nextToken();
            //log.debug(" - "+rname);

            Map m = (Map)map.get(rname);
            if (m == null) {
                m = new TreeMap();
                map.put(rname, m);
            }

            map = m;
        }

        Collection list = new ArrayList();
        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            String rname = (String)i.next();
            list.add(name+"."+rname);
        }

        return list;
    }

    public void clear() {
        loggers.clear();
    }
}
