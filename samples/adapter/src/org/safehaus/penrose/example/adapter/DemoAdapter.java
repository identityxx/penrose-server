package org.safehaus.penrose.example.adapter;

import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.ExceptionUtil;
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

    public void init() throws Exception {
        System.out.println("Initializing DemoAdapter.");

        RDNBuilder rb = new RDNBuilder();
        rb.set("cn", "Test User");
        rb.set("sn", "User");
        RDN pk = rb.toRdn();

        AttributeValues sourceValues = new AttributeValues();
        sourceValues.add("uid", "test");
        sourceValues.add("cn", "Penrose User");
        sourceValues.add("cn", "Test User");
        sourceValues.add("sn", "User");

        entries.put(pk, sourceValues);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            SourceConfig sourceConfig,
            RDN pk,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws LDAPException {

        String sourceName = sourceConfig.getName();
        System.out.println("Adding entry "+pk+" into "+sourceName+":");

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);
            System.out.println(" - "+name+": "+values);
        }

        if (entries.containsKey(pk)) {
            int rc = LDAPException.ENTRY_ALREADY_EXISTS;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        entries.put(pk, sourceValues);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            SourceConfig sourceConfig,
            RDN pk,
            String password,
            BindRequest request, BindResponse response) throws LDAPException {

        String sourceName = sourceConfig.getName();
        System.out.println("Binding to "+sourceName+" as "+pk+" with password "+password+".");

        AttributeValues sourceValues = (AttributeValues)entries.get(pk);
        if (sourceValues == null) {
            int rc = LDAPException.INVALID_CREDENTIALS;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        Object userPassword = sourceValues.getOne("userPassword");
        if (userPassword == null) {
            int rc = LDAPException.INVALID_CREDENTIALS;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        try {
            if (!PasswordUtil.comparePassword(password, userPassword)) {
                int rc = LDAPException.INVALID_CREDENTIALS;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

        } catch (Exception e) {
            int rc = LDAPException.INVALID_CREDENTIALS;
            String message = e.getMessage();
            log.debug("Error: "+message);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            SourceConfig sourceConfig,
            RDN pk,
            DeleteRequest request, DeleteResponse response) throws LDAPException {

        String sourceName = sourceConfig.getName();
        System.out.println("Deleting entry "+pk+" from "+sourceName+".");

        if (!entries.containsKey(pk)) {
            int rc = LDAPException.NO_SUCH_OBJECT;
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
        }

        entries.remove(pk);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            SourceConfig sourceConfig,
            RDN pk,
            Collection modifications,
            ModifyRequest request, ModifyResponse response) throws LDAPException {

        String sourceName = sourceConfig.getName();
        System.out.println("Modifying entry "+pk+" in "+sourceName+" with:");

        try {
            AttributeValues sourceValues = (AttributeValues)entries.get(pk);
            if (sourceValues == null) {
                int rc = LDAPException.NO_SUCH_OBJECT;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            for (Iterator i=modifications.iterator(); i.hasNext(); ) {
                Modification mi = (Modification)i.next();

                int type = mi.getType();
                Attribute attribute = mi.getAttribute();
                String name = attribute.getName();
                Collection values = attribute.getValues();

                switch (type) {
                    case Modification.ADD:
                        for (Iterator j=values.iterator(); j.hasNext(); ) {
                            Object value = j.next();
                            System.out.println(" - add "+name+": "+value);
                            sourceValues.add(name, value);
                        }
                        break;

                    case Modification.REPLACE:
                        sourceValues.remove(name);
                        for (Iterator j=values.iterator(); j.hasNext(); ) {
                            Object value = j.next();
                            System.out.println(" - replace "+name+": "+value);
                            sourceValues.add(name, value);
                        }
                        break;

                    case Modification.DELETE:
                        if (values.size() == 0) {
                            System.out.println(" - remove "+name);
                            sourceValues.remove(name);

                        } else {
                            for (Iterator j=values.iterator(); j.hasNext(); ) {
                                Object value = j.next();
                                System.out.println(" - remove "+name+": "+value);
                                sourceValues.remove(name, value);
                            }
                        }
                        break;
                }
            }

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    public void search(
            SourceConfig sourceConfig,
            Filter filter,
            SearchRequest searchRequest,
            SearchResponse response
    ) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Loading entries from source "+sourceName+" with filter "+filter+".");

        Collection attributes = searchRequest.getAttributes();
        for (Iterator i=entries.keySet().iterator(); i.hasNext(); ) {
            RDN pk = (RDN)i.next();
            AttributeValues sourceValues = (AttributeValues)entries.get(pk);

            if (!FilterTool.isValid(sourceValues, filter)) {
                System.out.println(" - "+pk+" => false");
                continue;
            }

            System.out.println(" - "+pk+" => true");

            if (attributes.size() == 0) {
                AttributeValues av = new AttributeValues();
                for (Iterator j=pk.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Object value = pk.get(name);
                    av.add("primaryKey."+name, value);
                }
                av.add(sourceValues);

                response.add(av);

            } else if (attributes.contains("dn")) {
                response.add(pk);
            }
        }

        response.close();
    }

}
