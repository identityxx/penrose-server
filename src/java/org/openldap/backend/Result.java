/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.openldap.backend;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public interface Result {

    public Iterator iterator();

    public int getReturnCode();

    public int size();

    public Collection getAll();

    public boolean hasNext();
    public Object next();

    public void close();

}
