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

import java.util.*;

import org.apache.log4j.Logger;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.connector.ConnectorConfig;
import org.safehaus.penrose.connector.AdapterConfig;
import org.safehaus.penrose.connector.JDBCAdapter;
import org.safehaus.penrose.connector.JNDIAdapter;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.schema.SchemaConfig;


/**
 * @author Endi S. Dewata
 */
public class DefaultPenroseConfig extends PenroseConfig {

    public DefaultPenroseConfig() {

        addSchemaConfig(new SchemaConfig("schema/autofs.schema"));
        addSchemaConfig(new SchemaConfig("schema/corba.schema"));
        addSchemaConfig(new SchemaConfig("schema/core.schema"));
        addSchemaConfig(new SchemaConfig("schema/cosine.schema"));
        addSchemaConfig(new SchemaConfig("schema/apache.schema"));
        addSchemaConfig(new SchemaConfig("schema/collective.schema"));
        addSchemaConfig(new SchemaConfig("schema/inetorgperson.schema"));
        addSchemaConfig(new SchemaConfig("schema/java.schema"));
        addSchemaConfig(new SchemaConfig("schema/krb5kdc.schema"));
        addSchemaConfig(new SchemaConfig("schema/nis.schema"));
        addSchemaConfig(new SchemaConfig("schema/system.schema"));
        addSchemaConfig(new SchemaConfig("schema/apachedns.schema"));

        AdapterConfig jdbcAdapterConfig = new AdapterConfig("JDBC", JDBCAdapter.class.getName());
        addAdapterConfig(jdbcAdapterConfig);

        AdapterConfig jndiAdapterConfig = new AdapterConfig("JNDI", JNDIAdapter.class.getName());
        addAdapterConfig(jndiAdapterConfig);

        PartitionConfig partitionConfig = new PartitionConfig("default", "conf");
        addPartitionConfig(partitionConfig);
    }
}
