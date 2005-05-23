/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.apacheds;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import java.util.List;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseEnumeration implements NamingEnumeration {

    public Iterator iterator;

    public PenroseEnumeration(List list) {
        this.iterator = list.iterator();
    }

    public void close() throws NamingException {
    }

    public boolean hasMore() throws NamingException {
        return iterator.hasNext();
    }

    public Object next() throws NamingException {
        return iterator.next();
    }

    public boolean hasMoreElements() {
        return iterator.hasNext();
    }

    public Object nextElement() {
        return iterator.next();
    }
}
