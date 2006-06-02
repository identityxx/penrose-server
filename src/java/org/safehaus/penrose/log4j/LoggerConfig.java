package org.safehaus.penrose.log4j;

import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class LoggerConfig {

    private String name;
    private boolean additivity;
    private String level;

    Set appenders = new LinkedHashSet();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isAdditivity() {
        return additivity;
    }

    public void setAdditivity(boolean additivity) {
        this.additivity = additivity;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public Collection getAppenders() {
        return appenders;
    }
    
    public void addAppender(String appenderName) {
        appenders.add(appenderName);
    }

    public void removeAppender(String appenderName) {
        appenders.remove(appenderName);
    }
}
