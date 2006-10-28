package org.safehaus.penrose.example.adapter;

import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.util.PasswordUtil;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.ModificationItem;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.NamingEnumeration;
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

        Row pk = new Row();
        pk.set("cn", "Test User");
        pk.set("sn", "User");

        AttributeValues sourceValues = new AttributeValues();
        sourceValues.add("uid", "test");
        sourceValues.add("cn", "Penrose User");
        sourceValues.add("cn", "Test User");
        sourceValues.add("sn", "User");

        entries.put(pk, sourceValues);
    }

    public int bind(SourceConfig sourceConfig, Row pk, String password) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Binding to "+sourceName+" as "+pk+" with password "+password+".");

        AttributeValues sourceValues = (AttributeValues)entries.get(pk);
        if (sourceValues == null) return LDAPException.INVALID_CREDENTIALS;

        Object userPassword = sourceValues.getOne("userPassword");
        if (userPassword == null) return LDAPException.INVALID_CREDENTIALS;

        if (!PasswordUtil.comparePassword(password, userPassword)) return LDAPException.INVALID_CREDENTIALS;

        return LDAPException.SUCCESS;
    }

    public void search(SourceConfig sourceConfig, Filter filter, PenroseSearchControls sc, PenroseSearchResults results) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Searching primary keys from source "+sourceName+" with filter "+filter+".");

        for (Iterator i=entries.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues sourceValues = (AttributeValues)entries.get(pk);

            if (!FilterTool.isValid(sourceValues, filter)) continue;

            results.add(pk);
        }

        results.close();
    }

    public void load(SourceConfig sourceConfig, Collection primaryKeys, Filter filter, PenroseSearchControls sc, PenroseSearchResults results) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Loading entries from source "+sourceName+" with filter "+filter+".");

        for (Iterator i=entries.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues sourceValues = (AttributeValues)entries.get(pk);

            if (!FilterTool.isValid(sourceValues, filter)) {
                System.out.println(" - "+pk+" => false");
                continue;
            }

            System.out.println(" - "+pk+" => true");

            AttributeValues av = new AttributeValues();
            for (Iterator j=pk.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = pk.get(name);
                av.add("primaryKey."+name, value);
            }
            av.add(sourceValues);

            results.add(av);
        }
    }

    public int add(SourceConfig sourceConfig, Row pk, AttributeValues sourceValues) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Adding entry "+pk+" into "+sourceName+":");

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);
            System.out.println(" - "+name+": "+values);
        }

        if (entries.containsKey(pk)) return LDAPException.ENTRY_ALREADY_EXISTS;

        entries.put(pk, sourceValues);

        return LDAPException.SUCCESS;
    }

    public int modify(SourceConfig sourceConfig, Row pk, Collection modifications) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Modifying entry "+pk+" in "+sourceName+" with:");

        AttributeValues sourceValues = (AttributeValues)entries.get(pk);
        if (sourceValues == null) return LDAPException.NO_SUCH_OBJECT;

        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            ModificationItem mi = (ModificationItem)i.next();
            Attribute attribute = (Attribute)mi.getAttribute();
            String name = attribute.getID();

            switch (mi.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                        Object value = j.next();
                        System.out.println(" - add "+name+": "+value);
                        sourceValues.add(name, value);
                    }
                    break;

                case DirContext.REPLACE_ATTRIBUTE:
                    sourceValues.remove(name);
                    for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                        Object value = j.next();
                        System.out.println(" - replace "+name+": "+value);
                        sourceValues.add(name, value);
                    }
                    break;

                case DirContext.REMOVE_ATTRIBUTE:
                    if (attribute.size() == 0) {
                        System.out.println(" - remove "+name);
                        sourceValues.remove(name);

                    } else {
                        for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                            Object value = j.next();
                            System.out.println(" - remove "+name+": "+value);
                            sourceValues.remove(name, value);
                        }
                    }
                    break;
            }
        }

        return LDAPException.SUCCESS;
    }

    public int delete(SourceConfig sourceConfig, Row pk) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Deleting entry "+pk+" from "+sourceName+".");

        if (!entries.containsKey(pk)) return LDAPException.NO_SUCH_OBJECT;

        entries.remove(pk);

        return LDAPException.SUCCESS;
    }
}
