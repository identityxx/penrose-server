package org.safehaus.penrose.log4j;

import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class RootConfig {

    String level;

    Set appenders = new LinkedHashSet();

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

    public void setAppenders(Collection appenderNames) {
        appenders.clear();
        appenders.addAll(appenderNames);
    }
}
