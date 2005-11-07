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
package org.safehaus.penrose.handler;

import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.acl.ACLEngine;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface HandlerContext {

    public String getRootDn() throws Exception;
    public String getRootPassword() throws Exception;

    public Collection getModules(String dn) throws Exception;
    public ACLEngine getACLEngine() throws Exception;
    public Engine getEngine() throws Exception;
    public Schema getSchema() throws Exception;
    public FilterTool getFilterTool() throws Exception;
    public Interpreter newInterpreter() throws Exception;
    public Config getConfig(String dn) throws Exception;
    public Collection getConfigs() throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
    public Connector getConnector() throws Exception;
}
