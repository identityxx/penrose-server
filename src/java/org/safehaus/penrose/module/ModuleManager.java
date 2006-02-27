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
package org.safehaus.penrose.module;

import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModuleManager {

    Logger log = Logger.getLogger(ModuleManager.class);

    private Penrose penrose;

    private Map modules = new LinkedHashMap();

    public void init() throws Exception {
        PartitionManager partitionManager = penrose.getPartitionManager();
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            for (Iterator j=partition.getModuleConfigs().iterator(); j.hasNext(); ) {
                ModuleConfig moduleConfig = (ModuleConfig)j.next();
                init(partition, moduleConfig);
            }
        }
    }

    public void init(Partition partition, ModuleConfig moduleConfig) throws Exception {

        if (!moduleConfig.isEnabled()) return;
        
        Module module = getModule(moduleConfig.getName());
        if (module != null) return;
        
        Class clazz = Class.forName(moduleConfig.getModuleClass());
        module = (Module)clazz.newInstance();

        module.setModuleConfig(moduleConfig);
        module.setPartition(partition);
        module.setPenrose(penrose);
        module.init();

        addModule(moduleConfig.getName(), module);
    }

    public void start() throws Exception {
        //log.debug("Starting Modules...");
        for (Iterator i=getServiceNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            start(name);
        }
        //log.debug("Modules started.");
    }

    public void start(String name) throws Exception {

        Module module = getModule(name);
        if (module == null) throw new Exception(name+" not found.");

        ModuleConfig moduleConfig = module.getModuleConfig();
        if (!moduleConfig.isEnabled()) return;

        log.debug("Starting "+name+" module.");
        module.start();
    }

    public void stop() throws Exception {
        log.debug("Stopping Modules...");
        Collection list = getServiceNames();
        String names[] = (String[])list.toArray(new String[list.size()]);

        for (int i=names.length-1; i>=0; i--) {
            String name = names[i];
            stop(name);
        }
        log.debug("Modules stopped.");
    }

    public void stop(String name) throws Exception {

        Module module = getModule(name);
        if (module == null) throw new Exception(name+" not found.");

        ModuleConfig moduleConfig = module.getModuleConfig();
        if (!moduleConfig.isEnabled()) return;

        log.debug("Stopping "+name+" module.");
        module.stop();
    }

    public String getStatus(String name) throws Exception {
        Module module = getModule(name);
        if (module == null) throw new Exception(name+" not found.");
        return module.getStatus();
    }

    public void addModule(String name, Module module) {
        modules.put(name, module);
    }

    public Module getModule(String name) {
        return (Module)modules.get(name);
    }

    public Collection getServiceNames() {
        return modules.keySet();
    }

    public Collection getModules() {
        return modules.values();
    }

    public Module removeModule(String name) {
        return (Module)modules.remove(name);
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
        log.debug("Find matching module mapping for "+dn);

        Collection list = new ArrayList();

        PartitionManager partitionManager = penrose.getPartitionManager();
        Partition partition = partitionManager.getPartitionByDn(dn);
        if (partition == null) return list;

        for (Iterator i = partition.getModuleMappings().iterator(); i.hasNext(); ) {
            Collection c = (Collection)i.next();

            for (Iterator j=c.iterator(); j.hasNext(); ) {
                ModuleMapping moduleMapping = (ModuleMapping)j.next();

                String moduleName = moduleMapping.getModuleName();
                Module module = (Module)modules.get(moduleName);

                if (moduleMapping.match(dn)) {
                    log.debug(" - "+moduleName);
                    list.add(module);
                }
            }
        }

        return list;
    }

}
