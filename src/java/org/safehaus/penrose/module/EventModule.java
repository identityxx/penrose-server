/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.module;


import org.safehaus.penrose.event.*;

/**
 * @author Endi S. Dewata
 */
public class EventModule extends GenericModule {

    public void beforeBind(BindEvent event) throws Exception {
        System.out.println("[EventModule] Before bind");
        System.out.println("[EventModule] dn: "+event.getDn());
    }

    public void afterBind(BindEvent event) throws Exception {
        System.out.println("[EventModule] After bind");
        System.out.println("[EventModule] dn: "+event.getDn());
        System.out.println("[EventModule] rc: "+event.getReturnCode());
    }

    public void beforeUnbind(BindEvent event) throws Exception {
        System.out.println("[EventModule] Before unbind");
    }

    public void afterUnbind(BindEvent event) throws Exception {
        System.out.println("[EventModule] After unbind");
        System.out.println("[EventModule] rc: "+event.getReturnCode());
    }

    public void beforeAdd(AddEvent event) throws Exception {
        System.out.println("[EventModule] Before add");
        System.out.println("[EventModule] dn: "+event.getEntry().getDN());
    }

    public void afterAdd(AddEvent event) throws Exception {
        System.out.println("[EventModule] After add");
        System.out.println("[EventModule] dn: "+event.getEntry().getDN());
        System.out.println("[EventModule] rc: "+event.getReturnCode());
    }

    public void beforeModify(ModifyEvent event) throws Exception {
        System.out.println("[EventModule] Before modify");
        System.out.println("[EventModule] dn: "+event.getDn());
    }

    public void afterModify(ModifyEvent event) throws Exception {
        System.out.println("[EventModule] After modify");
        System.out.println("[EventModule] dn: "+event.getDn());
        System.out.println("[EventModule] rc: "+event.getReturnCode());
    }

    public void beforeDelete(DeleteEvent event) throws Exception {
        System.out.println("[EventModule] Before delete");
        System.out.println("[EventModule] dn: "+event.getDn());
    }

    public void afterDelete(DeleteEvent event) throws Exception {
        System.out.println("[EventModule] After delete");
        System.out.println("[EventModule] dn: "+event.getDn());
        System.out.println("[EventModule] rc: "+event.getReturnCode());
    }

    public void beforeSearch(SearchEvent event) throws Exception {
        System.out.println("[EventModule] Before search");
        System.out.println("[EventModule] base: "+event.getBase());
    }

    public void afterSearch(SearchEvent event) throws Exception {
        System.out.println("[EventModule] After search");
        System.out.println("[EventModule] base: "+event.getBase());
        System.out.println("[EventModule] rc: "+event.getReturnCode());
    }
}
