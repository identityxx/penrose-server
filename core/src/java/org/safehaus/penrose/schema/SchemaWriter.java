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
package org.safehaus.penrose.schema;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SchemaWriter {

    public Logger log = LoggerFactory.getLogger(getClass());

    File baseDir;

    public SchemaWriter() {
    }

    public SchemaWriter(File directory) {
        this.baseDir = directory;
    }

    public void write(Schema schema) throws Exception {
        File file = new File(baseDir, schema.getSchemaConfig().getPath());
        write(file, schema);
    }

    public void write(String path, Schema schema) throws Exception {
        write(new File(path), schema);
    }

    public void write(File file, Schema schema) throws Exception {
        file.getParentFile().mkdirs();

        PrintWriter out = new PrintWriter(new FileWriter(file), true);

        Collection attributeTypes = schema.getAttributeTypes();
        for (Iterator i=attributeTypes.iterator(); i.hasNext(); ) {
            AttributeType at = (AttributeType)i.next();
            write(out, at);
        }

        Collection objectClasses = schema.getObjectClasses();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            ObjectClass oc = (ObjectClass)i.next();
            write(out, oc);
        }

        out.close();
    }

    public void write(PrintWriter out, AttributeType at) throws Exception {
        out.print("attributetype ( ");
        out.print(at.toString(true));
        out.println(")");
        out.println();

/*
        out.println("attributetype ( "+at.getOid());

        Collection names = at.getNames();
        if (names.size() == 1) {
            out.println("    NAME '"+names.iterator().next()+"'");

        } else if (names.size() > 1) {
            out.print("    NAME ( ");
            for (Iterator i=names.iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                out.print("'"+name+"' ");
            }
            out.println(")");
        }

        if (at.getDescription() != null) {
            out.println("    DESC '"+escape(at.getDescription())+"'");
        }

        if (at.isObsolete()) {
            out.println("    OBSOLETE");
        }

        if (at.getSuperClass() != null) {
            out.println("    SUP "+at.getSuperClass());
        }

        if (at.getEquality() != null) {
            out.println("    EQUALITY "+at.getEquality());
        }

        if (at.getOrdering() != null) {
            out.println("    ORDERING "+at.getOrdering());
        }

        if (at.getSubstring() != null) {
            out.println("    SUBSTR "+at.getSubstring());
        }

        if (at.getSyntax() != null) {
            out.println("    SYNTAX "+at.getSyntax());
        }

        if (at.isSingleValued()) {
            out.println("    SINGLE-VALUE");
        }

        if (at.isCollective()) {
            out.println("    COLLECTIVE");
        }

        if (!at.isModifiable()) {
            out.println("    NO-USER-MODIFICATION");
        }

        if (at.getUsage() != null && !AttributeType.USER_APPLICATIONS.equals(at.getUsage())) {
            out.println("    USAGE "+at.getUsage());
        }

        out.println(")");
        out.println();
*/
    }

    public void write(PrintWriter out, ObjectClass oc) throws Exception {
        out.print("objectclass ( ");
        out.print(oc.toString(true));
        out.println(")");
        out.println();

/*
        out.println("objectclass ( "+oc.getOid());

        Collection names = oc.getNames();
        if (names.size() == 1) {
            out.println("    NAME '"+names.iterator().next()+"'");

        } else if (names.size() > 1) {
            out.print("    NAME ( ");
            for (Iterator i=names.iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                out.print("'"+name+"' ");
            }
            out.println(")");
        }

        if (oc.getDescription() != null) {
            out.println("    DESC '"+escape(oc.getDescription())+"'");
        }

        if (oc.isObsolete()) {
            out.println("    OBSOLETE");
        }

        Collection superClasses = oc.getSuperClasses();
        if (superClasses.size() == 1) {
            out.println("    SUP "+superClasses.iterator().next());

        } else if (superClasses.size() > 1) {
            out.print("    SUP ( ");
            for (Iterator i=superClasses.iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                out.print(name);
                if (i.hasNext()) out.print(" $ ");
            }
            out.println(" )");
        }

        if (!ObjectClass.STRUCTURAL.equals(oc.getType())) {
            out.println("    "+oc.getType());
        }

        Collection requiredAttributes = oc.getRequiredAttributes();
        if (requiredAttributes.size() == 1) {
            out.println("    MUST "+requiredAttributes.iterator().next());

        } else if (requiredAttributes.size() > 1) {
            out.print("    MUST ( ");
            for (Iterator i=requiredAttributes.iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                out.print(name);
                if (i.hasNext()) out.print(" $ ");
            }
            out.println(" )");
        }

        Collection optionalAttributes = oc.getOptionalAttributes();
        if (optionalAttributes.size() == 1) {
            out.println("    MAY "+optionalAttributes.iterator().next());

        } else if (optionalAttributes.size() > 1) {
            out.print("    MAY ( ");
            for (Iterator i=optionalAttributes.iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                out.print(name);
                if (i.hasNext()) out.print(" $ ");
            }
            out.println(" )");
        }

        out.println(")");
        out.println();
*/
    }

    public static String escape(String s) {
        StringBuilder sb = new StringBuilder();

        for (int i=0; i<s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'' || c == '\\') {
                sb.append('\\');
                sb.append(toHex(c));
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    public static String toHex(char c) {
        String s = Integer.toHexString(c);
        return s.length() == 1 ? '0'+s : s;
    }
}
