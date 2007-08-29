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
package org.safehaus.penrose.example.module;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;

import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class DemoModule extends Module implements DemoModuleMBean {

    private String attribute;

    public void init() throws Exception {
        System.out.println("#### Initializing DemoModule.");
    }

    public void beforeAdd(AddEvent event) throws Exception {
        AddRequest request = event.getRequest();
        System.out.println("#### Adding "+request.getDn()+":");

        Attributes attributes = request.getAttributes();
        for (Attribute attribute : attributes.getAll()) {
            String name = attribute.getName();
            for (Object value : attribute.getValues()) {
                System.out.println(" - " + name + ": " + value);
            }
        }

        // change sn attribute to upper case
        String sn = (String)attributes.getValue("sn");
        attributes.setValue("sn", sn.toUpperCase());
    }

    public void afterAdd(AddEvent event) throws Exception {
        AddResponse response = event.getResponse();
        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Add succeded.");
        } else {
            System.out.println("#### Add failed. RC="+rc);
        }
    }

    public void beforeBind(BindEvent event) throws Exception {
        BindRequest request = event.getRequest();
        System.out.println("#### Binding as "+request.getDn()+" with password "+request.getPassword()+".");
    }

    public void afterBind(BindEvent event) throws Exception {
        BindRequest request = event.getRequest();
        BindResponse response = event.getResponse();
        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Bound as "+request.getDn()+".");
        } else {
            System.out.println("#### Failed to bind as "+request.getDn()+". RC="+rc);
        }
    }

    public void beforeDelete(DeleteEvent event) throws Exception {
        DeleteRequest request = event.getRequest();
        System.out.println("#### Deleting "+request.getDn());
    }

    public void afterDelete(DeleteEvent event) throws Exception {
        DeleteResponse response = event.getResponse();
        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Delete succeded.");
        } else {
            System.out.println("#### Delete failed. RC="+rc);
        }
    }

    public void beforeModify(ModifyEvent event) throws Exception {
        ModifyRequest request = event.getRequest();
        System.out.println("#### Modifying "+request.getDn()+":");

        for (Modification mi : request.getModifications()) {

            int type = mi.getType();
            Attribute attribute = mi.getAttribute();
            String name = attribute.getName();

            switch (type) {
                case Modification.ADD:
                    System.out.println(" - add: " + name);
                    break;
                case Modification.DELETE:
                    System.out.println(" - delete: " + name);
                    break;
                case Modification.REPLACE:
                    System.out.println(" - replace: " + name);
                    break;
            }

            for (Object value : attribute.getValues()) {
                System.out.println("   " + name + ": " + value);
            }
        }
    }

    public void afterModify(ModifyEvent event) throws Exception {
        ModifyResponse response = event.getResponse();
        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Modify succeded.");
        } else {
            System.out.println("#### Modify failed. RC="+rc);
        }
    }

    public void beforeSearch(SearchEvent event) throws Exception {
        SearchRequest request = event.getRequest();
        System.out.println("#### Searching "+request.getDn()+".");

        Filter filter = request.getFilter();
        if (filter != null && filter.toString().equalsIgnoreCase("(cn=secret)")) {
            throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
        }

        SearchResponse<SearchResult> response = event.getResponse();

        // register result listener
        response.addListener(new SearchResponseAdapter() {
            public void postAdd(SearchResponseEvent event) {
                SearchResult result = (SearchResult)event.getObject();
                DN dn = result.getDn();
                System.out.println("#### Returning "+dn+".");
            }
        });
    }

    public void afterSearch(SearchEvent event) throws Exception {
        SearchResponse response = event.getResponse();
        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Search returned "+response.getTotalCount()+" entries.");
        } else {
            System.out.println("#### Search failed. RC="+rc);
        }
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String operation() throws Exception {
        System.out.println("#### Running DemoModule.");
        return "DemoModule";
    }

    public String operation(String... params) throws Exception {
        System.out.println("#### Running DemoModule: "+ Arrays.asList(params));
        return "DemoModule: "+Arrays.asList(params);
    }
}