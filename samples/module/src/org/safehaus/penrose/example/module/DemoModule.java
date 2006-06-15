package org.safehaus.penrose.example.module;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class DemoModule extends Module {

    public void init() throws Exception {
        System.out.println("Initializing DemoModule.");
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

    public void beforeSearch(SearchEvent event) throws Exception {
        System.out.println("Searching "+event.getBaseDn()+" with filter "+event.getFilter()+".");

        PenroseSearchResults results = event.getSearchResults();

        // register result listener
        results.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                SearchResult sr = (SearchResult)event.getObject();
                String dn = sr.getName();
                System.out.println("Returning "+dn+".");
            }
        });
    }

    public void afterSearch(SearchEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("Search succeded.");
        } else {
            System.out.println("Search failed. RC="+rc);
        }
    }

    public void beforeAdd(AddEvent event) throws Exception {
        System.out.println("Adding "+event.getDn()+":");

        Attributes attributes = event.getAttributes();
        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                System.out.println(" - "+name+": "+value);
            }
        }

        // change sn attribute to upper case
        Attribute sn = attributes.get("sn");
        String value = (String)sn.get();
        sn.clear();
        sn.add(value.toUpperCase());
    }

    public void afterAdd(AddEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("Add succeded.");
        } else {
            System.out.println("Add failed. RC="+rc);
        }
    }

    public void beforeModify(ModifyEvent event) throws Exception {
        System.out.println("Modifying "+event.getDn()+":");

        Collection modifications = event.getModifications();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            ModificationItem mi = (ModificationItem)i.next();
            Attribute attribute = mi.getAttribute();
            String name = attribute.getID();

            switch (mi.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    System.out.println(" - add: "+name);
                    break;
                case DirContext.REMOVE_ATTRIBUTE:
                    System.out.println(" - delete: "+name);
                    break;
                case DirContext.REPLACE_ATTRIBUTE:
                    System.out.println(" - replace: "+name);
                    break;
            }

            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                System.out.println("   "+name+": "+value);
            }
        }
    }

    public void afterModify(ModifyEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("Modify succeded.");
        } else {
            System.out.println("Modify failed. RC="+rc);
        }
    }

    public void beforeDelete(DeleteEvent event) throws Exception {
        System.out.println("Deleting "+event.getDn());
    }

    public void afterDelete(DeleteEvent event) throws Exception {
        int rc = event.getReturnCode();
        if (rc == LDAPException.SUCCESS) {
            System.out.println("Delete succeded.");
        } else {
            System.out.println("Delete failed. RC="+rc);
        }
    }
}