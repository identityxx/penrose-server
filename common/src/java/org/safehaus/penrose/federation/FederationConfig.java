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
package org.safehaus.penrose.federation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class FederationConfig implements Serializable, Cloneable {

    protected Map<String, Repository> repositories = new TreeMap<String, Repository>();

    public FederationConfig() {
    }

    public void clear() {
        repositories.clear();
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

        FederationConfig federationConfig = (FederationConfig)object;

        if (!equals(repositories, federationConfig.repositories)) return false;

        return true;
    }

    public void copy(FederationConfig federationConfig) throws CloneNotSupportedException {

        repositories = new LinkedHashMap<String, Repository>();
        for (Repository repository : federationConfig.repositories.values()) {
            addRepository((Repository) repository.clone());
        }
    }

    public Object clone() throws CloneNotSupportedException {

        FederationConfig federationConfig = (FederationConfig)super.clone();
        federationConfig.copy(this);

        return federationConfig;
    }

    public Collection<Repository> getRepositories() {
        return repositories.values();
    }

    public Repository getRepository(String name) {
        return repositories.get(name);
    }

    public Repository removeRepository(String name) {
        return repositories.remove(name);
    }

    public void addRepository(Repository repository) {

        Logger log = LoggerFactory.getLogger(getClass());

        String name = repository.getName();
        String type = repository.getType();

        log.debug("Adding "+name+" ("+type+")");

        if ("GLOBAL".equals(type)) {
            repository = new GlobalRepository(repository);

        } else if ("LDAP".equals(type)) {
            repository = new LDAPRepository(repository);

        } else if ("NIS".equals(type)) {
            repository = new NISDomain(repository);
        }

        repositories.put(name, repository);
    }

    public Collection<String> getRepositoryNames() {
        Collection<String> list = new ArrayList<String>();
        list.addAll(repositories.keySet());
        return list;
    }
}