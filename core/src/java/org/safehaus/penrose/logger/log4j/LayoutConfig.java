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

import java.util.Collection;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * @author Endi S. Dewata
 */
public class LayoutConfig {

    String layoutClass;
    Map parameters = new LinkedHashMap();

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public String getParameter(String name) {
        return (String)parameters.get(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public String removeParameter(String name) {
        return (String)parameters.remove(name);
    }

    public void clearParameters() {
        parameters.clear();
    }

    public String getLayoutClass() {
        return layoutClass;
    }

    public void setLayoutClass(String layoutClass) {
        this.layoutClass = layoutClass;
    }
}
