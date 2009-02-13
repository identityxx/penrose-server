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
package org.safehaus.penrose.log.log4j;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Log4jConfig implements Serializable, Cloneable {

    boolean debug;

    Map<String,AppenderConfig> appenderConfigs = new LinkedHashMap<String,AppenderConfig>();
    Map<String,LoggerConfig> loggerConfigs = new LinkedHashMap<String,LoggerConfig>();

    RootLoggerConfig rootLoggerConfig;

    public Collection<AppenderConfig> getAppenderConfigs() {
        return appenderConfigs.values();
    }

    public Collection<String> getAppenderConfigNames() {
        return appenderConfigs.keySet();
    }

    public void setAppenderConfigs(Collection<AppenderConfig> appenderConfigs) {
        this.appenderConfigs.clear();
        if (appenderConfigs == null) return;
        for (AppenderConfig appenderConfig : appenderConfigs) {
            addAppenderConfig(appenderConfig);
        }
    }

    public void addAppenderConfig(AppenderConfig appenderConfig) {
        appenderConfigs.put(appenderConfig.getName(), appenderConfig);
    }

    public AppenderConfig getAppenderConfig(String name) {
        return appenderConfigs.get(name);
    }

    public AppenderConfig removeAppenderConfig(String name) {
        return appenderConfigs.remove(name);
    }

    public Collection<LoggerConfig> getLoggerConfigs() {
        return loggerConfigs.values();
    }

    public Collection<String> getLoggerConfigNames() {
        return loggerConfigs.keySet();
    }

    public void setLoggerConfigs(Collection<LoggerConfig> loggerConfigs) {
        this.loggerConfigs.clear();
        if (loggerConfigs == null) return;
        for (LoggerConfig loggerConfig : loggerConfigs) {
            addLoggerConfig(loggerConfig);
        }
    }

    public void addLoggerConfig(LoggerConfig loggerConfig) {
        loggerConfigs.put(loggerConfig.getName(), loggerConfig);
    }

    public LoggerConfig getLoggerConfig(String name) {
        return loggerConfigs.get(name);
    }

    public LoggerConfig removeLoggerConfig(String name) {
        return loggerConfigs.remove(name);
    }

    public RootLoggerConfig getRootLoggerConfig() {
        return rootLoggerConfig;
    }

    public void setRootLoggerConfig(RootLoggerConfig rootLoggerConfig) {
        this.rootLoggerConfig = rootLoggerConfig;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public int hashCode() {
        return (debug ? 0 : 1)
                + appenderConfigs.hashCode()
                + loggerConfigs.hashCode()
                + (rootLoggerConfig == null ? 0 : rootLoggerConfig.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Log4jConfig log4jConfig = (Log4jConfig)object;
        if (debug != log4jConfig.debug) return false;
        if (!equals(appenderConfigs, log4jConfig.appenderConfigs)) return false;
        if (!equals(loggerConfigs, log4jConfig.loggerConfigs)) return false;
        if (!equals(rootLoggerConfig, log4jConfig.rootLoggerConfig)) return false;

        return true;
    }

    public void copy(Log4jConfig log4jConfig) throws CloneNotSupportedException {
        debug = log4jConfig.debug;

        appenderConfigs = new LinkedHashMap<String,AppenderConfig>();
        appenderConfigs.putAll(log4jConfig.appenderConfigs);

        loggerConfigs = new LinkedHashMap<String,LoggerConfig>();
        loggerConfigs.putAll(log4jConfig.loggerConfigs);

        rootLoggerConfig = log4jConfig.rootLoggerConfig == null ? null : (RootLoggerConfig)log4jConfig.rootLoggerConfig.clone();
    }

    public Object clone() throws CloneNotSupportedException {
        Log4jConfig log4jConfig = (Log4jConfig)super.clone();
        log4jConfig.copy(this);
        return log4jConfig;
    }
}
