/**
 * Copyright 2009 Red Hat, Inc.
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

import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.jdbc.adapter.JDBCAdapter;
import org.safehaus.penrose.ldap.adapter.LDAPAdapter;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.nis.adapter.NISAdapter;

/**
 * @author Endi S. Dewata
 */
public class DefaultPenroseConfig extends PenroseConfig {

    public DefaultPenroseConfig() throws Exception {
        addAdapterConfig(new AdapterConfig("JDBC", JDBCAdapter.class.getName()));
        addAdapterConfig(new AdapterConfig("LDAP", LDAPAdapter.class.getName()));
        addAdapterConfig(new AdapterConfig("NIS", NISAdapter.class.getName()));
    }
}
