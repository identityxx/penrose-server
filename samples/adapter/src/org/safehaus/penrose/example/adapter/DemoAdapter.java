package org.safehaus.penrose.example.adapter;

import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.schema.SchemaManager;
import org.ietf.ldap.LDAPException;

import java.util.Iterator;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

/**
 * @author Endi S. Dewata
 */

public class DemoAdapter extends Adapter {

    Map entries = new HashMap();
    FilterTool filterTool;

    public void init() throws Exception {
        System.out.println("Initializing DemoAdapter.");

        SchemaManager schemaManager = penroseContext.getSchemaManager();

        filterTool = new FilterTool();
        filterTool.setSchemaManager(schemaManager);

        RDNBuilder rb = new RDNBuilder();
        rb.set("cn", "Test User");
        rb.set("sn", "User");
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
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();
        Attributes attributes = request.getAttributes();

        System.out.println("Adding entry "+dn);

        RDN rdn = dn.getRdn();

        if (entries.containsKey(rdn)) {
            int rc = LDAPException.ENTRY_ALREADY_EXISTS;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        entries.put(rdn, attributes);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();
        String password = request.getPassword();

        System.out.println("Binding as "+dn+" with password "+password+".");

        RDN rdn = dn.getRdn();

        Attributes attributes = (Attributes)entries.get(rdn);
        if (attributes == null) {
            int rc = LDAPException.NO_SUCH_OBJECT;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        Object userPassword = attributes.getValue("userPassword");
        if (userPassword == null) {
            int rc = LDAPException.NO_SUCH_ATTRIBUTE;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        if (!userPassword.equals(password)) {
            int rc = LDAPException.INVALID_CREDENTIALS;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();
        System.out.println("Deleting entry "+dn);

        RDN rdn = dn.getRdn();

        if (!entries.containsKey(rdn)) {
            int rc = LDAPException.NO_SUCH_OBJECT;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        entries.remove(rdn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();
        Collection modifications = request.getModifications();
        System.out.println("Modifying entry "+dn);

        RDN rdn = dn.getRdn();

        Attributes attributes = (Attributes)entries.get(rdn);
        if (attributes == null) {
            int rc = LDAPException.NO_SUCH_OBJECT;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String name = attribute.getName();
            Collection values = attribute.getValues();

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
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();
        RDN newRdn = request.getNewRdn();
        boolean deleteOldRdn = request.getDeleteOldRdn();

        System.out.println("Renaming entry "+dn+" to "+newRdn);

        RDN rdn = dn.getRdn();

        Attributes attributes = (Attributes)entries.remove(rdn);
        if (attributes == null) {
            int rc = LDAPException.NO_SUCH_OBJECT;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        if (deleteOldRdn) {
            for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Object value = newRdn.get(name);
                attributes.removeValue(name, value);
            }
        }

        for (Iterator i=newRdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = newRdn.get(name);
            attributes.addValue(name, value);
        }

        entries.put(rdn, attributes);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();

        DN dn = request.getDn();
        Filter filter = request.getFilter();

        System.out.println("Searching "+dn+" with filter "+filter+".");

        for (Iterator i=entries.keySet().iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            Attributes attributes = (Attributes)entries.get(rdn);

            if (!filterTool.isValid(attributes, filter)) {
                System.out.println(" - "+rdn+" => false");
                continue;
            }

            System.out.println(" - "+rdn+" => true");

            Entry result = new Entry(rdn, entryMapping);
            result.setAttributes(sourceMapping.getName(), attributes);
            response.add(result);
        }

        response.close();
    }

}
