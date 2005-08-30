/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose;

import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SearchResults {

    public Logger log = LoggerFactory.getLogger(getClass());

    public List results = new ArrayList();
    public boolean done = false;

    private int returnCode = LDAPException.SUCCESS;
    
    public synchronized void add(Object object) {
        results.add(object);
        notifyAll();
    }

    public synchronized void addAll(Collection collection) {
        results.addAll(collection);
        notifyAll();
    }

    public synchronized Object next() {

        while (!done && results.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }

        if (results.size() == 0) return null;

        return results.remove(0);
    }

    public synchronized void close() {
        done = true;
        notifyAll();
    }

    public synchronized Collection getAll() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }

        return results;
    }

    public synchronized int size() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }

        return results.size();
    }

    public synchronized Iterator iterator() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }

        return results.iterator();
    }

    public synchronized int getReturnCode() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }

        return returnCode;
    }

    public synchronized boolean hasNext() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }

        return results.size() > 0;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

}
