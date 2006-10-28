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
package org.safehaus.penrose.log4j;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.Iterator;
import java.io.File;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Log4jConfig implements Cloneable, Serializable {

    boolean debug;

    Map appenderConfigs = new LinkedHashMap();
    Map loggerConfigs = new LinkedHashMap();

    RootConfig rootConfig;

    public Collection getAppenderConfigs() {
        return appenderConfigs.values();
    }

    public Collection getAppenderConfigNames() {
        return appenderConfigs.keySet();
    }

    public void addAppenderConfig(AppenderConfig appenderConfig) {
        appenderConfigs.put(appenderConfig.getName(), appenderConfig);
    }

    public AppenderConfig getAppenderConfig(String name) {
        return (AppenderConfig)appenderConfigs.get(name);
    }

    public AppenderConfig removeAppenderConfig(String name) {
        return (AppenderConfig)appenderConfigs.remove(name);
    }

    public Collection getLoggerConfigs() {
        return loggerConfigs.values();
    }

    public Collection getLoggerConfigNames() {
        return loggerConfigs.keySet();
    }

    public void addLoggerConfig(LoggerConfig loggerConfig) {
        loggerConfigs.put(loggerConfig.getName(), loggerConfig);
    }

    public LoggerConfig getLoggerConfig(String name) {
        return (LoggerConfig)loggerConfigs.get(name);
    }

    public LoggerConfig removeLoggerConfig(String name) {
        return (LoggerConfig)loggerConfigs.remove(name);
    }

    public RootConfig getRootConfig() {
        return rootConfig;
    }

    public void setRootConfig(RootConfig rootConfig) {
        this.rootConfig = rootConfig;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public static void main(String args[]) throws Exception {

        Log4jConfigReader configReader = new Log4jConfigReader(new File(args[0]));
        Log4jConfig config = configReader.read();

        Log4jConfigWriter configWriter = new Log4jConfigWriter();
        configWriter.write(config);
    }

    public int hashCode() {
        return (debug ? 0 : 1) +
                (appenderConfigs == null ? 0 : appenderConfigs.hashCode()) +
                (loggerConfigs == null ? 0 : loggerConfigs.hashCode()) +
                (rootConfig == null ? 0 : rootConfig.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        Log4jConfig log4jConfig = (Log4jConfig)object;
        if (debug != log4jConfig.debug) return false;
        if (!equals(appenderConfigs, log4jConfig.appenderConfigs)) return false;
        if (!equals(loggerConfigs, log4jConfig.loggerConfigs)) return false;
        if (!equals(rootConfig, log4jConfig.rootConfig)) return false;

        return true;
    }

    public void copy(Log4jConfig log4jConfig) {
        debug = log4jConfig.debug;

        appenderConfigs.clear();
        for (Iterator i=appenderConfigs.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            AppenderConfig appenderConfig = (AppenderConfig)log4jConfig.appenderConfigs.get(name);
            appenderConfigs.put(name, appenderConfig.clone());
        }

        loggerConfigs.clear();
        for (Iterator i=loggerConfigs.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            LoggerConfig loggerConfig = (LoggerConfig)log4jConfig.loggerConfigs.get(name);
            loggerConfigs.put(name, loggerConfig.clone());
        }

        rootConfig = (RootConfig)log4jConfig.rootConfig.clone();
    }

    public Object clone() {
        Log4jConfig log4jConfig = new Log4jConfig();
        log4jConfig.copy(this);
        return log4jConfig;
    }
}
