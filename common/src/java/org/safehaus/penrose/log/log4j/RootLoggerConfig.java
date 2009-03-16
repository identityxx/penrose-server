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

import java.util.Collection;
import java.util.LinkedList;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class RootLoggerConfig implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    String level;
    Collection<String> appenderNames = new LinkedList<String>();

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public Collection<String> getAppenderNames() {
        return appenderNames;
    }

    public void addAppender(String appenderName) {
        appenderNames.add(appenderName);
    }

    public void removeAppender(String appenderName) {
        appenderNames.remove(appenderName);
    }

    public void setAppenderNames(Collection<String> appenderNames) {
        if (appenderNames == this.appenderNames) return;
        this.appenderNames.clear();
        if (appenderNames == null) return;
        this.appenderNames.addAll(appenderNames);
    }

    public int hashCode() {
        return level == null ? 0 : level.hashCode();
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

        RootLoggerConfig rootConfig = (RootLoggerConfig)object;
        if (!equals(level, rootConfig.level)) return false;
        if (!equals(appenderNames, rootConfig.appenderNames)) return false;

        return true;
    }

    public void copy(RootLoggerConfig rootConfig) throws CloneNotSupportedException {
        level = rootConfig.level;

        appenderNames = new LinkedList<String>();
        appenderNames.addAll(rootConfig.appenderNames);
    }

    public Object clone() throws CloneNotSupportedException {
        RootLoggerConfig rootConfig = (RootLoggerConfig)super.clone();
        rootConfig.copy(this);
        return rootConfig;
    }
}
