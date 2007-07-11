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

    Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;
    private SessionContext sessionContext;

    private Map<String,Map<String,Module>> modules = new LinkedHashMap<String,Map<String,Module>>();

    public void init(Partition partition, ModuleConfig moduleConfig) throws Exception {

        Module module = getModule(partition.getName(), moduleConfig.getName());
        if (module != null) return;
        
        if (!moduleConfig.isEnabled()) return;

        log.debug("Initializing module "+moduleConfig.getName()+".");
        
        Class clazz = Class.forName(moduleConfig.getModuleClass());
        module = (Module)clazz.newInstance();

        module.setModuleConfig(moduleConfig);
        module.setPartition(partition);
        module.setPenroseConfig(penroseConfig);
        module.setPenroseContext(penroseContext);
        module.setSessionContext(sessionContext);
        module.init();

        addModule(partition.getName(), module);
    }

    public void start() throws Exception {
        log.debug("Starting modules...");

        for (String partitionName : modules.keySet()) {
            Map<String, Module> map = modules.get(partitionName);

            for (String moduleName : map.keySet()) {
                Module module = map.get(moduleName);

                ModuleConfig moduleConfig = module.getModuleConfig();
                if (!moduleConfig.isEnabled()) {
                    log.debug("Module " + moduleConfig.getName() + " is disabled");
                    continue;
                }

                module.start();
            }
        }

        log.debug("Modules started.");
    }

    public void start(String partitionName, String moduleName) throws Exception {

        Module module = getModule(partitionName, moduleName);
        if (module == null) {
            log.debug("Module "+moduleName+" not found");
            return;
        }

        ModuleConfig moduleConfig = module.getModuleConfig();
        if (!moduleConfig.isEnabled()) {
            log.debug("Module "+moduleConfig.getName()+" is disabled");
            return;
        }

        log.debug("Starting "+moduleName +" module.");
        module.start();
    }

    public void stop() throws Exception {
        log.debug("Stopping modules...");

        for (String partitionName : modules.keySet()) {
            Map<String,Module> map = modules.get(partitionName);

            for (String moduleName : map.keySet()) {
                Module module = map.get(moduleName);

                ModuleConfig moduleConfig = module.getModuleConfig();
                if (!moduleConfig.isEnabled()) {
                    log.debug("Module " + moduleConfig.getName() + " is disabled");
                    continue;
                }

                module.stop();
            }
        }

        log.debug("Modules stopped.");
    }

    public void stop(String partitionName, String moduleName) throws Exception {

        Module module = getModule(partitionName, moduleName);
        if (module == null) {
            log.debug("Module "+moduleName+" not found");
            return;
        }

        ModuleConfig moduleConfig = module.getModuleConfig();
        if (!moduleConfig.isEnabled()) {
            log.debug("Module "+moduleConfig.getName()+" is disabled");
            return;
        }

        log.debug("Stopping "+moduleName +" module.");
        module.stop();
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

    public Module getModule(String partitionName, String moduleName) {
        Map map = (Map)modules.get(partitionName);
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
        Map map = (Map)modules.get(partitionName);
        if (map == null) return null;
        return (Module)map.remove(moduleName);
    }

    public void clear() {
        modules.clear();
    }

    public Collection<Module> getModules(DN dn) throws Exception {

        //log.debug("Finding matching modules for \""+dn+"\".");

        Collection<Module> list = new ArrayList<Module>();

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        Partition partition = partitionManager.getPartition(dn);
        
        if (partition == null) return list;

        for (Collection<ModuleMapping> moduleMappings : partition.getModules().getModuleMappings()) {

            for (Iterator j = moduleMappings.iterator(); j.hasNext();) {
                ModuleMapping moduleMapping = (ModuleMapping) j.next();
                if (!moduleMapping.match(dn)) continue;

                String moduleName = moduleMapping.getModuleName();
                Module module = getModule(partition.getName(), moduleName);

                //log.debug(" - "+moduleName);
                list.add(module);
            }
        }

        //log.debug("Found "+list.size()+" module(s).");

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
