package org.safehaus.penrose.event;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleManager;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class EventManager {

    private ModuleManager moduleManager;

    public void postEvent(String dn, AddEvent event) throws Exception {

        Collection c = moduleManager.getModules(dn);

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Module module = (Module)i.next();

            switch (event.getType()) {
                case AddEvent.BEFORE_ADD:
                    module.beforeAdd(event);
                    break;

                case AddEvent.AFTER_ADD:
                    module.afterAdd(event);
                    break;
            }
        }
    }

    public void postEvent(String dn, BindEvent event) throws Exception {

        Collection c = moduleManager.getModules(dn);

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Module module = (Module)i.next();

            switch (event.getType()) {
                case BindEvent.BEFORE_BIND:
                    module.beforeBind(event);
                    break;

                case BindEvent.AFTER_BIND:
                    module.afterBind(event);
                    break;

                case BindEvent.BEFORE_UNBIND:
                    module.beforeUnbind(event);
                    break;

                case BindEvent.AFTER_UNBIND:
                    module.afterUnbind((BindEvent)event);
                    break;
            }
        }
    }

    public void postEvent(String dn, CompareEvent event) throws Exception {

        Collection c = moduleManager.getModules(dn);

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Module module = (Module)i.next();

            switch (event.getType()) {
                case CompareEvent.BEFORE_COMPARE:
                    module.beforeCompare(event);
                    break;

                case CompareEvent.AFTER_COMPARE:
                    module.afterCompare(event);
                    break;
            }
        }
    }

    public void postEvent(String dn, DeleteEvent event) throws Exception {

        Collection c = moduleManager.getModules(dn);

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Module module = (Module)i.next();

            switch (event.getType()) {
                case DeleteEvent.BEFORE_DELETE:
                    module.beforeDelete((DeleteEvent)event);
                    break;

                case DeleteEvent.AFTER_DELETE:
                    module.afterDelete((DeleteEvent)event);
                    break;
            }
        }
    }

    public void postEvent(String dn, ModifyEvent event) throws Exception {

        Collection c = moduleManager.getModules(dn);

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Module module = (Module)i.next();

            switch (event.getType()) {
            case ModifyEvent.BEFORE_MODIFY:
                module.beforeModify(event);
                break;

            case ModifyEvent.AFTER_MODIFY:
                module.afterModify(event);
                break;
            }
        }
    }

    public void postEvent(String dn, ModRdnEvent event) throws Exception {

        Collection c = moduleManager.getModules(dn);

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Module module = (Module)i.next();

            switch (event.getType()) {
            case ModRdnEvent.BEFORE_MODRDN:
                module.beforeModRdn(event);
                break;

            case ModRdnEvent.AFTER_MODRDN:
                module.afterModRdn(event);
                break;
            }
        }
    }

    public void postEvent(String dn, SearchEvent event) throws Exception {

        Collection c = moduleManager.getModules(dn);

        for (Iterator i=c.iterator(); i.hasNext(); ) {
            Module module = (Module)i.next();

            switch (event.getType()) {
                case SearchEvent.BEFORE_SEARCH:
                    module.beforeSearch(event);
                    break;

                case SearchEvent.AFTER_SEARCH:
                    module.afterSearch(event);
                    break;
            }
        }
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }
}
