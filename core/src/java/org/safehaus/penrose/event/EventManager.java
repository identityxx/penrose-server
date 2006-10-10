package org.safehaus.penrose.event;

import org.safehaus.penrose.module.ModuleManager;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class EventManager {

    private ModuleManager moduleManager;

    public Collection addListeners = new ArrayList();
    public Collection bindListeners = new ArrayList();
    public Collection compareListeners = new ArrayList();
    public Collection deleteListeners = new ArrayList();
    public Collection modifyListeners = new ArrayList();
    public Collection modrdnListeners = new ArrayList();
    public Collection searchListeners = new ArrayList();

    public boolean postEvent(String dn, AddEvent event) throws Exception {

        Collection listeners = moduleManager.getModules(dn);
        listeners.addAll(addListeners);

        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            AddListener listener = (AddListener)i.next();

            switch (event.getType()) {
                case AddEvent.BEFORE_ADD:
                    boolean b = listener.beforeAdd(event);
                    if (!b) return false;
                    break;

                case AddEvent.AFTER_ADD:
                    listener.afterAdd(event);
                    break;
            }
        }

        return true;
    }

    public boolean postEvent(String dn, BindEvent event) throws Exception {

        Collection listeners = moduleManager.getModules(dn);
        listeners.addAll(bindListeners);

        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            BindListener listener = (BindListener)i.next();

            switch (event.getType()) {
                case BindEvent.BEFORE_BIND:
                    boolean b = listener.beforeBind(event);
                    if (!b) return false;
                    break;

                case BindEvent.AFTER_BIND:
                    listener.afterBind(event);
                    break;

                case BindEvent.BEFORE_UNBIND:
                    b = listener.beforeUnbind(event);
                    if (!b) return false;
                    break;

                case BindEvent.AFTER_UNBIND:
                    listener.afterUnbind((BindEvent)event);
                    break;
            }
        }

        return true;
    }

    public boolean postEvent(String dn, CompareEvent event) throws Exception {

        Collection listeners = moduleManager.getModules(dn);
        listeners.addAll(compareListeners);

        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            CompareListener listener = (CompareListener)i.next();

            switch (event.getType()) {
                case CompareEvent.BEFORE_COMPARE:
                    boolean b = listener.beforeCompare(event);
                    if (!b) return false;
                    break;

                case CompareEvent.AFTER_COMPARE:
                    listener.afterCompare(event);
                    break;
            }
        }

        return true;
    }

    public boolean postEvent(String dn, DeleteEvent event) throws Exception {

        Collection listeners = moduleManager.getModules(dn);
        listeners.addAll(deleteListeners);

        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            DeleteListener listener = (DeleteListener)i.next();

            switch (event.getType()) {
                case DeleteEvent.BEFORE_DELETE:
                    boolean b = listener.beforeDelete((DeleteEvent)event);
                    if (!b) return false;
                    break;

                case DeleteEvent.AFTER_DELETE:
                    listener.afterDelete((DeleteEvent)event);
                    break;
            }
        }

        return true;
    }

    public boolean postEvent(String dn, ModifyEvent event) throws Exception {

        Collection listeners = moduleManager.getModules(dn);
        listeners.addAll(modifyListeners);

        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            ModifyListener listener = (ModifyListener)i.next();

            switch (event.getType()) {
            case ModifyEvent.BEFORE_MODIFY:
                boolean b = listener.beforeModify(event);
                if (!b) return false;
                break;

            case ModifyEvent.AFTER_MODIFY:
                listener.afterModify(event);
                break;
            }
        }

        return true;
    }

    public boolean postEvent(String dn, ModRdnEvent event) throws Exception {

        Collection listeners = moduleManager.getModules(dn);
        listeners.addAll(modrdnListeners);

        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            ModRdnListener listener = (ModRdnListener)i.next();

            switch (event.getType()) {
            case ModRdnEvent.BEFORE_MODRDN:
                boolean b = listener.beforeModRdn(event);
                if (!b) return false;
                break;

            case ModRdnEvent.AFTER_MODRDN:
                listener.afterModRdn(event);
                break;
            }
        }

        return true;
    }

    public boolean postEvent(String dn, SearchEvent event) throws Exception {

        Collection listeners = moduleManager.getModules(dn);
        listeners.addAll(searchListeners);

        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            SearchListener listener = (SearchListener)i.next();

            switch (event.getType()) {
                case SearchEvent.BEFORE_SEARCH:
                    boolean b = listener.beforeSearch(event);
                    if (!b) return false;
                    break;

                case SearchEvent.AFTER_SEARCH:
                    listener.afterSearch(event);
                    break;
            }
        }

        return true;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    public void addAddListener(AddListener listener) {
        if (!addListeners.contains(listener)) addListeners.add(listener);
    }

    public void removeAddListener(AddListener listener) {
        addListeners.remove(listener);
    }

    public void addBindListener(BindListener listener) {
        if (!bindListeners.contains(listener)) bindListeners.add(listener);
    }

    public void removeBindListener(BindListener listener) {
        bindListeners.remove(listener);
    }

    public void addCompareListener(CompareListener listener) {
        if (!compareListeners.contains(listener)) compareListeners.add(listener);
    }

    public void removeCompareListener(CompareListener listener) {
        compareListeners.remove(listener);
    }

    public void addDeleteListener(DeleteListener listener) {
        if (!deleteListeners.contains(listener)) deleteListeners.add(listener);
    }

    public void removeDeleteListener(DeleteListener listener) {
        deleteListeners.remove(listener);
    }

    public void addModifyListener(ModifyListener listener) {
        if (!modifyListeners.contains(listener)) modifyListeners.add(listener);
    }

    public void removeModifyListener(ModifyListener listener) {
        modifyListeners.remove(listener);
    }

    public void addModRdnListener(ModRdnListener listener) {
        if (!modrdnListeners.contains(listener)) modrdnListeners.add(listener);
    }

    public void removeModRdnListener(ModRdnListener listener) {
        modrdnListeners.remove(listener);
    }

    public void addSearchListener(SearchListener listener) {
        if (!searchListeners.contains(listener)) searchListeners.add(listener);
    }

    public void removeSearchListener(SearchListener listener) {
        searchListeners.remove(listener);
    }

}
