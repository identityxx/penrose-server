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
package org.safehaus.penrose.management;

import org.safehaus.penrose.ldap.DN;

import java.util.*;

public interface PenroseServiceMBean {

    public String getProductName() throws Exception;
    public String getProductVersion() throws Exception;

    public String getHome() throws Exception;

    public void start() throws Exception;
    public void stop() throws Exception;
    public void reload() throws Exception;
    public void restart() throws Exception;

    public Collection<String> getPartitionNames() throws Exception;
    public Collection<String> getServiceNames() throws Exception;
    public void start(String serviceName) throws Exception;
    public void stop(String serviceName) throws Exception;
    public String getStatus(String serviceName) throws Exception;

    public byte[] download(String filename) throws Exception;
	public void upload(String filename, byte content[]) throws Exception;
    public Collection<String> listFiles(String directory) throws Exception;

    public Collection<String> getLoggerNames() throws Exception;
    public String getLoggerLevel(String name) throws Exception;
    public void setLoggerLevel(String name, String level) throws Exception;
}
