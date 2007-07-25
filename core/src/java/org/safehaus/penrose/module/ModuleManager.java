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
package org.safehaus.penrose.module;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.SessionContext;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModuleManager implements ModuleManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;
    private SessionContext sessionContext;

    private Map<String,Map<String,Module>> modules = new LinkedHashMap<String,Map<String,Module>>();

    public Module init(Partition partition, ModuleConfig moduleConfig) throws Exception {

        if (!moduleConfig.isEnabled()) return null;

        Module module = getModule(partition.getName(), moduleConfig.getName());
        if (module != null) return module;
        
        if (debug) log.debug("Initializing module "+moduleConfig.getName()+".");

        String moduleClass = moduleConfig.getModuleClass();
        ClassLoader cl = partition.getClassLoader();
        Class clazz = cl.loadClass(moduleClass);
        module = (Module)clazz.newInstance();

        module.setModuleConfig(moduleConfig);
        module.setPartition(partition);
        module.setPenroseConfig(penroseConfig);
        module.setPenroseContext(penroseContext);
        module.setSessionContext(sessionContext);
        module.init();

        addModule(partition.getName(), module);

        return module;
    }

    public String getStatus(String partitionName, String moduleName) throws Exception {
        Module module = getModule(partitionName, moduleName);
        if (module == null) throw new Exception(moduleName +" not found.");
        return module.getStatus();
    }

    public void addModule(String partitionName, Module module) {
        Map<String,Module> map = modules.get(partitionName);
        if (map == null) {
            map = new TreeMap<String,Module>();
            modules.put(partitionName, map);
        }
        map.put(module.getName(), module);
    }

    public Module getModule(Partition partition, String moduleName) {
        return getModule(partition.getName(), moduleName);
    }

    public Module getModule(String partitionName, String moduleName) {
        Map map = modules.get(partitionName);
        if (map == null) return null;
        return (Module)map.get(moduleName);
    }

    public Collection<String> getPartitionNames() {
        return new ArrayList<String>(modules.keySet()); // return Serializable list
    }

    public Collection<String> getModuleNames(String partitionName) {
        Map<String,Module> map = modules.get(partitionName);
        if (map == null) return new ArrayList<String>();
        return new ArrayList<String>(map.keySet()); // return Serializable list
    }

    public Module removeModule(String partitionName, String moduleName) {
        Map map = modules.get(partitionName);
        if (map == null) return null;
        return (Module)map.remove(moduleName);
    }

    public void clear() {
        modules.clear();
    }

    public Collection<Module> getModules(Partition partition, DN dn) throws Exception {

        if (debug) log.debug("Modules:");

        Collection<Module> list = new ArrayList<Module>();
        if (partition == null) return list;

        for (Collection<ModuleMapping> moduleMappings : partition.getModules().getModuleMappings()) {

            for (ModuleMapping moduleMapping : moduleMappings) {
                String moduleName = moduleMapping.getModuleName();

                boolean b = moduleMapping.match(dn);
                if (debug) log.debug(" - "+moduleName+": "+b);

                if (!b) continue;

                Module module = getModule(partition.getName(), moduleName);
                list.add(module);
            }
        }

        return list;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }
}
