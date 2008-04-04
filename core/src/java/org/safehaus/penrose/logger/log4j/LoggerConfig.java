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
package org.safehaus.penrose.logger.log4j;

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

    public void setAppenders(Collection appenderNames) {
        appenders.clear();
        appenders.addAll(appenderNames);
    }
}
