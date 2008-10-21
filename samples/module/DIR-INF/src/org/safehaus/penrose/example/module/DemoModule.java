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

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleChain;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SearchOperation;
import org.safehaus.penrose.util.BinaryUtil;

import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class DemoModule extends Module implements DemoModuleMBean {

    private String attribute;

    public void init() throws Exception {
        System.out.println("#### Initializing DemoModule.");
    }

    public void add(
            Session session,
            AddRequest request,
            AddResponse response,
            ModuleChain chain
    ) throws Exception {

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

        chain.add(session, request, response);

        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Add succeded.");
        } else {
            System.out.println("#### Add failed. RC="+rc);
        }
    }

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response,
            ModuleChain chain
    ) throws Exception {

        System.out.println("#### Binding as "+request.getDn()+" with password "+ BinaryUtil.encode(request.getPassword())+".");

        chain.bind(session, request, response);

        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Bound as "+request.getDn()+".");
        } else {
            System.out.println("#### Failed to bind as "+request.getDn()+". RC="+rc);
        }
    }

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response,
            ModuleChain chain
    ) throws Exception {

        System.out.println("#### Deleting "+request.getDn());

        chain.delete(session, request, response);

        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Delete succeded.");
        } else {
            System.out.println("#### Delete failed. RC="+rc);
        }
    }

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response,
            ModuleChain chain
    ) throws Exception {

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

        chain.modify(session, request, response);

        int rc = response.getReturnCode();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Modify succeded.");
        } else {
            System.out.println("#### Modify failed. RC="+rc);
        }
    }

    public void search(
            final SearchOperation operation,
            final ModuleChain chain
    ) throws Exception {

        System.out.println("#### Searching "+operation.getDn()+".");

        Filter filter = operation.getFilter();
        if (filter != null && filter.toString().equalsIgnoreCase("(cn=secret)")) {
            throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
        }

        SearchOperation op = new SearchOperation(operation) {
            public void add(SearchResult result) throws Exception {
                System.out.println("#### Returning "+result.getDn()+".");
                super.add(result);
            }
        };
        
        chain.search(op);

        int rc = op.waitFor();
        if (rc == LDAP.SUCCESS) {
            System.out.println("#### Search returned "+operation.getTotalCount()+" entries.");
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