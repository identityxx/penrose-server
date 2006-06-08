package org.safehaus.penrose.example.adapter;

import org.safehaus.penrose.connector.Adapter;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.ietf.ldap.LDAPException;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */

public class DemoAdapter extends Adapter {

    public int bind(SourceConfig sourceConfig, AttributeValues sourceValues, String password) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Binding to "+sourceName+" with password "+password+".");

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = (Collection)i.next();
            System.out.println(" - "+name+": "+values);
        }

        return LDAPException.SUCCESS;
    }

    public void search(SourceConfig sourceConfig, Filter filter, long sizeLimit, PenroseSearchResults results) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Searching primary keys from source "+sourceName+" with filter "+filter+".");

        results.close();
    }

    public void load(SourceConfig sourceConfig, Filter filter, long sizeLimit, PenroseSearchResults results) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Loading entries from source "+sourceName+" with filter "+filter+".");

        results.close();
    }

    public AttributeValues get(SourceConfig sourceConfig, Row pk) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Getting entry from source "+sourceName+" with primary key "+pk+".");

        return null;
    }

    public int add(SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Adding entry into "+sourceName+":");

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = (Collection)i.next();
            System.out.println(" - "+name+": "+values);
        }

        return LDAPException.SUCCESS;
    }

    public int modify(SourceConfig sourceConfig, AttributeValues oldSourceValues, AttributeValues newSourceValues) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Replacing entry in "+sourceName+":");

        for (Iterator i=oldSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = (Collection)i.next();
            System.out.println(" - "+name+": "+values);
        }

        System.out.println("with:");

        for (Iterator i=newSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = (Collection)i.next();
            System.out.println(" - "+name+": "+values);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {

        String sourceName = sourceConfig.getName();
        System.out.println("Deleting entry from "+sourceName+":");

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = (Collection)i.next();
            System.out.println(" - "+name+": "+values);
        }

        return LDAPException.SUCCESS;
    }
}
