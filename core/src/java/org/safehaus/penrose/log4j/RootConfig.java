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

import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Collection;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class RootConfig implements Cloneable, Serializable {

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

    public int hashCode() {
        return (level == null ? 0 : level.hashCode()) +
                (appenders == null ? 0 : appenders.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        RootConfig rootConfig = (RootConfig)object;
        if (!equals(level, rootConfig.level)) return false;
        if (!equals(appenders, rootConfig.appenders)) return false;

        return true;
    }

    public void copy(RootConfig rootConfig) {
        level = rootConfig.level;

        appenders.clear();
        appenders.addAll(rootConfig.appenders);
    }

    public Object clone() {
        RootConfig rootConfig = new RootConfig();
        rootConfig.copy(this);
        return rootConfig;
    }
}
