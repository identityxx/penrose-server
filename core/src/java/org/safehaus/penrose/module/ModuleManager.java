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

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModuleManager implements ModuleManagerMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    private Penrose penrose;

    private Map modules = new LinkedHashMap();

    public void load(Collection partitions) throws Exception {
        for (Iterator i=partitions.iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            for (Iterator j=partition.getModuleConfigs().iterator(); j.hasNext(); ) {
                ModuleConfig moduleConfig = (ModuleConfig)j.next();
                load(partition, moduleConfig);
            }
        }
    }

    public void load(Partition partition, ModuleConfig moduleConfig) throws Exception {

        Module module = getModule(partition.getName(), moduleConfig.getName());
        if (module != null) return;
        
        if (!moduleConfig.isEnabled()) return;

        log.debug("Initializing module "+moduleConfig.getName());
        
        Class clazz = Class.forName(moduleConfig.getModuleClass());
        module = (Module)clazz.newInstance();

        module.setModuleConfig(moduleConfig);
        module.setPartition(partition);
        module.setPenrose(penrose);
        module.init();

        addModule(partition.getName(), module);
    }

    public void start() throws Exception {
        log.debug("Starting modules...");

        for (Iterator i=modules.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)modules.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String moduleName = (String)j.next();
                Module module = (Module)map.get(moduleName);

                ModuleConfig moduleConfig = module.getModuleConfig();
                if (!moduleConfig.isEnabled()) {
                    log.debug("Module "+moduleConfig.getName()+" is disabled");
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

        for (Iterator i=modules.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)modules.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String moduleName = (String)j.next();
                Module module = (Module)map.get(moduleName);

                ModuleConfig moduleConfig = module.getModuleConfig();
                if (!moduleConfig.isEnabled()) {
                    log.debug("Module "+moduleConfig.getName()+" is disabled");
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
        Map map = (Map)modules.get(partitionName);
        if (map == null) {
            map = new TreeMap();
            modules.put(partitionName, map);
        }
        map.put(module.getName(), module);
    }

    public Module getModule(String partitionName, String moduleName) {
        Map map = (Map)modules.get(partitionName);
        if (map == null) return null;
        return (Module)map.get(moduleName);
    }

    public Collection getPartitionNames() {
        return new ArrayList(modules.keySet()); // return Serializable list
    }

    public Collection getModuleNames(String partitionName) {
        Map map = (Map)modules.get(partitionName);
        if (map == null) return new ArrayList();
        return new ArrayList(map.keySet()); // return Serializable list
    }

    public Module removeModule(String partitionName, String moduleName) {
        Map map = (Map)modules.get(partitionName);
        if (map == null) return null;
        return (Module)map.remove(moduleName);
    }

    public void clear() {
        modules.clear();
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public Collection getModules(String dn) throws Exception {

        //log.debug("Finding matching modules for \""+dn+"\".");

        Collection list = new ArrayList();

        PartitionManager partitionManager = penrose.getPartitionManager();
        Partition partition = partitionManager.getPartitionByDn(dn);
        
        if (partition == null) return list;

        for (Iterator i = partition.getModuleMappings().iterator(); i.hasNext(); ) {
            Collection c = (Collection)i.next();

            for (Iterator j=c.iterator(); j.hasNext(); ) {
                ModuleMapping moduleMapping = (ModuleMapping)j.next();
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

}
