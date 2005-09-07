/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.schema;

import org.apache.ldap.server.tools.schema.DirectorySchemaTool;
import org.apache.ldap.server.schema.bootstrap.AbstractBootstrapSchema;

import java.lang.reflect.Method;

/**
 * @author Endi S. Dewata
 */
public class SchemaGenerator {

    public static void main(String args[]) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage:");
            System.out.println("    schema.bat <dir> <file.schema>");
            System.out.println("    schema.sh <dir> <file.schema>");
            System.exit(0);
        }

        String schemaDir = args[0];
        String name = args[1];

        DirectorySchemaTool tool = new DirectorySchemaTool();

        String owner = "uid=admin,ou=system";
        String pkg = "org.apache.ldap.server.schema.bootstrap";
        String dependencies[] = new String[] { "system", "core" };

        System.out.println("Generating schema classes for "+name+".schema");

        AbstractBootstrapSchema schema = new AbstractBootstrapSchema(owner, name, pkg, dependencies) {};
        tool.setSchema(schema);
        tool.setSchemaSrcDir(schemaDir);
        tool.setSchemaTargetDir(schemaDir);
        tool.generate();
    }
}
