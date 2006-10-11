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
package org.safehaus.penrose.util;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map;

public class ClassRegistry extends ClassLoader {

    Logger log = LoggerFactory.getLogger(getClass());

    Map classes = new TreeMap();

    public ClassRegistry() {
        this(null);
    }

    public ClassRegistry(ClassLoader parent) {
        super(parent);
    }

    public Class findClass(String name) throws ClassNotFoundException {
        Class clazz = (Class)classes.get(name);
        if (clazz != null) return clazz;

        byte[] bytes = loadClassData(name);
        clazz = defineClass(name, bytes, 0, bytes.length);
        classes.put(name, clazz);
        
        return clazz;
    }

    protected byte[] loadClassData(String name) throws ClassNotFoundException {
        throw new ClassNotFoundException(name);
    }

    protected byte[] merge(Collection list) {

        int length = 0;
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            byte[] b = (byte[])i.next();
            length += b.length;
        }

        //log.debug("Preparing "+length+" bytes array");
        byte[] bytes = new byte[length];

        int counter = 0;
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            byte[] b = (byte[])i.next();
            //log.debug("Copying "+b.length+" bytes");
            for (int j=0; j<b.length; j++) bytes[counter++] = b[j];
        }

        return bytes;
    }
}
