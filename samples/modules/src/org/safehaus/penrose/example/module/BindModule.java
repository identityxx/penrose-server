package org.safehaus.penrose.example.module;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.BindEvent;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class BindModule extends Module {

    public void init() throws Exception {
        System.out.println("Initializing BindModule.");
    }

    public void beforeBind(BindEvent event) throws Exception {
        System.out.println("Binding as "+event.getDn()+" with password "+event.getPassword()+".");
    }

    public void afterBind(BindEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("Bound as "+event.getDn()+".");
        } else {
            System.out.println("Failed to bind as "+event.getDn()+". RC="+rc);
        }
    }
}