package org.safehaus.penrose.example.adapter;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.partition.Partition;

import java.util.Map;
import java.util.HashMap;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */

public class DemoConnection extends Connection {

    Map<RDN,Attributes> entries = new HashMap<RDN,Attributes>();

    public void init() throws Exception {
        System.out.println("Initializing DemoConnection.");

        RDNBuilder rb = new RDNBuilder();
        rb.set("uid", "test");
        RDN rdn = rb.toRdn();

        Attributes attributes = new Attributes();
        attributes.addValue("uid", "test");
        attributes.addValue("cn", "Penrose User");
        attributes.addValue("cn", "Test User");
        attributes.addValue("sn", "User");

        entries.put(rdn, attributes);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Source source,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();
        Attributes attributes = request.getAttributes();

        System.out.println("Adding entry "+dn);

        RDN rdn = dn.getRdn();

        if (entries.containsKey(rdn)) {
            throw LDAP.createException(LDAP.ENTRY_ALREADY_EXISTS);
        }

        entries.put(rdn, attributes);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Source source,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();
        String password = new String(request.getPassword());

        System.out.println("Binding as "+dn+" with password "+password+".");

        RDN rdn = dn.getRdn();

        Attributes attributes = entries.get(rdn);
        if (attributes == null) {
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Object userPassword = attributes.getValue("userPassword");
        if (userPassword == null) {
            throw LDAP.createException(LDAP.NO_SUCH_ATTRIBUTE);
        }

        if (!PasswordUtil.validate(password, userPassword)) {
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Source source,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();
        System.out.println("Deleting entry "+dn);

        RDN rdn = dn.getRdn();

        if (!entries.containsKey(rdn)) {
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        entries.remove(rdn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Source source,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();
        Collection<Modification> modifications = request.getModifications();
        System.out.println("Modifying entry "+dn);

        RDN rdn = dn.getRdn();

        Attributes attributes = entries.get(rdn);
        if (attributes == null) {
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        for (Modification modification : modifications) {

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String name = attribute.getName();
            Collection<Object> values = attribute.getValues();

            switch (type) {
                case Modification.ADD:
                    attributes.addValues(name, values);
                    break;

                case Modification.REPLACE:
                    attributes.setValues(name, values);
                    break;

                case Modification.DELETE:
                    if (values.size() == 0) {
                        attributes.remove(name);

                    } else {
                        attributes.removeValues(name, values);
                    }
                    break;
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Source source,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();
        RDN newRdn = request.getNewRdn();
        boolean deleteOldRdn = request.getDeleteOldRdn();

        System.out.println("Renaming entry "+dn+" to "+newRdn);

        RDN rdn = dn.getRdn();

        Attributes attributes = entries.remove(rdn);
        if (attributes == null) {
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        if (deleteOldRdn) {
            for (String name : rdn.getNames()) {
                Object value = newRdn.get(name);
                attributes.removeValue(name, value);
            }
        }

        for (String name : newRdn.getNames()) {
            Object value = newRdn.get(name);
            attributes.addValue(name, value);
        }

        entries.put(rdn, attributes);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Source source,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        Filter filter = request.getFilter();

        System.out.println("Searching with filter "+filter+".");

        response.setSizeLimit(request.getSizeLimit());

        Partition partition = connectionContext.getPartition();
        PenroseContext penroseContext = partition.getPartitionContext().getPenroseContext();
        FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();

        for (RDN rdn : entries.keySet()) {
            Attributes attributes = entries.get(rdn);

            if (!filterEvaluator.eval(attributes, filter)) {
                System.out.println("Entry " + rdn + " doesn't match.");
                continue;
            }

            System.out.println("Found entry " + rdn + ".");
            response.add(new SearchResult(new DN(rdn), (Attributes)attributes.clone()));
        }

        response.close();
    }
}
