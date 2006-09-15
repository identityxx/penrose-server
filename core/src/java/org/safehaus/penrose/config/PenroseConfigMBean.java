/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
package org.safehaus.penrose.config;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public interface PenroseConfigMBean {

    public String getHome() throws Exception;
    public void setHome(String home) throws Exception;

    public Map getSystemProperties() throws Exception;
    public String getSystemProperty(String name) throws Exception;
    public void setSystemProperty(String name, String value) throws Exception;
    public String removeSystemProperty(String name) throws Exception;

    public Collection getServiceNames() throws Exception;

    public Collection getAdapterNames() throws Exception;
    public Collection getSchemaNames() throws Exception;
    public Collection getPartitionNames() throws Exception;

    public String getRootDn() throws Exception;
    public void setRootDn(String rootDn) throws Exception;

    public String getRootPassword() throws Exception;
    public void setRootPassword(String rootPassword) throws Exception;
}
