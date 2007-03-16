package org.safehaus.penrose.example.module;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Attribute;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class DemoModule extends Module {

    public void init() throws Exception {
        System.out.println("#### Initializing DemoModule.");
    }

    public boolean beforeBind(BindEvent event) throws Exception {
        BindRequest request = event.getRequest();
        System.out.println("#### Binding as "+request.getDn()+" with password "+request.getPassword()+".");
        return true;
    }

    public void afterBind(BindEvent event) throws Exception {
        BindRequest request = event.getRequest();
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("#### Bound as "+request.getDn()+".");
        } else {
            System.out.println("#### Failed to bind as "+request.getDn()+". RC="+rc);
        }
    }

    public boolean beforeSearch(SearchEvent event) throws Exception {
        SearchRequest request = event.getRequest();
        System.out.println("#### Searching "+request.getDn()+" with filter "+request.getFilter()+".");

        if (request.getFilter().toString().equalsIgnoreCase("(cn=secret)")) {
            return false;
        }

        SearchResponse response = event.getResponse();

        // register result listener
        response.addListener(new SearchResponseAdapter() {
            public void postAdd(SearchResponseEvent event) {
                SearchResult sr = (SearchResult)event.getObject();
                Entry entry = sr.getEntry();
                DN dn = entry.getDn();
                System.out.println("Returning "+dn+".");
            }
        });

        return true;
    }

    public void afterSearch(SearchEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("#### Search succeded.");
        } else {
            System.out.println("#### Search failed. RC="+rc);
        }
    }

    public boolean beforeAdd(AddEvent event) throws Exception {
        AddRequest request = event.getRequest();
        System.out.println("#### Adding "+request.getDn()+":");

        Attributes attributes = request.getAttributes();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getName();
            Collection values = attribute.getValues();
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                System.out.println(" - "+name+": "+value);
            }
        }

        // change sn attribute to upper case
        String sn = (String)attributes.getValue("sn");
        attributes.setValue("sn", sn.toUpperCase());

        return true;
    }

    public void afterAdd(AddEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("#### Add succeded.");
        } else {
            System.out.println("#### Add failed. RC="+rc);
        }
    }

    public boolean beforeModify(ModifyEvent event) throws Exception {
        ModifyRequest request = event.getRequest();
        System.out.println("#### Modifying "+request.getDn()+":");

        Collection modifications = request.getModifications();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification mi = (Modification)i.next();

            int type = mi.getType();
            Attribute attribute = mi.getAttribute();
            String name = attribute.getName();

            switch (type) {
                case Modification.ADD:
                    System.out.println(" - add: "+name);
                    break;
                case Modification.DELETE:
                    System.out.println(" - delete: "+name);
                    break;
                case Modification.REPLACE:
                    System.out.println(" - replace: "+name);
                    break;
            }

            for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
                Object value = j.next();
                System.out.println("   "+name+": "+value);
            }
        }

        return true;
    }

    public void afterModify(ModifyEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("#### Modify succeded.");
        } else {
            System.out.println("#### Modify failed. RC="+rc);
        }
    }

    public boolean beforeDelete(DeleteEvent event) throws Exception {
        DeleteRequest request = event.getRequest();
        System.out.println("#### Deleting "+request.getDn());
        return true;
    }

    public void afterDelete(DeleteEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("#### Delete succeded.");
        } else {
            System.out.println("#### Delete failed. RC="+rc);
        }
    }
}