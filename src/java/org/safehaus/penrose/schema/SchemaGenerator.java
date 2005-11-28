/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import org.apache.ldap.server.tools.schema.DirectorySchemaTool;
import org.apache.ldap.server.schema.bootstrap.AbstractBootstrapSchema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.jar.JarEntry;

import com.sun.tools.javac.Main;

/**
 * @author Endi S. Dewata
 */
public class SchemaGenerator {

    File schemaDir;
    String name;

    File sourceDir;
    File output;

    public SchemaGenerator(File file) throws Exception {
        this.schemaDir = file.getParentFile();
        this.name = file.getName();

        int i = name.lastIndexOf(".");
        if (i >= 0) name = name.substring(0, i);

        sourceDir = File.createTempFile(name, null, schemaDir);
        sourceDir.delete();
        sourceDir.mkdir();

        output = new File(schemaDir, name+".jar");
        if (output.exists()) output.delete();

    }

    public void run() throws Exception {
        generate();
        compile();
        createJar();
        delete();
    }

    public void generate() throws Exception {
        String owner = "uid=admin,ou=system";
        String pkg = "org.apache.ldap.server.schema.bootstrap";
        String dependencies[] = new String[] { "system", "core" };

        AbstractBootstrapSchema schema = new AbstractBootstrapSchema(owner, name, pkg, dependencies) {};

        DirectorySchemaTool tool = new DirectorySchemaTool();
        tool.setSchema(schema);
        tool.setSchemaSrcDir(schemaDir.getAbsolutePath());
        tool.setSchemaTargetDir(sourceDir.getAbsolutePath());
        tool.generate();
    }

    public void compile() throws Exception {
        compile(sourceDir.getPath(), sourceDir);
    }

    public void compile(String prefix, File file) throws Exception {

        File files[] = file.listFiles();
        if (files != null) {
            for (int i=0; i<files.length; i++) {
                compile(prefix, files[i]);
            }
        }

        if (file.isDirectory()) return;
        if (!file.getName().endsWith(".java")) return;

        //String path = file.getPath().substring(prefix.length()+1);
        //System.out.println("Compiling "+path);

        Main.compile(new String[] { file.getAbsolutePath() });
    }

    public void createJar() throws Exception {
        Manifest manifest = new Manifest();
        JarOutputStream os = new JarOutputStream(new FileOutputStream(output), manifest);

        addJarEntries(sourceDir.getPath(), sourceDir, os);

        os.close();
    }

    public void addJarEntries(String prefix, File file, JarOutputStream os) throws Exception {

        File files[] = file.listFiles();
        if (files != null) {
            for (int i=0; i<files.length; i++) {
                addJarEntries(prefix, files[i], os);
            }
        }

        if (file.isDirectory()) return;

        String path = file.getPath().substring(prefix.length()+1);
        path = path.replace('\\', '/');
        if (path.endsWith(".java")) return;

        //System.out.println("Adding "+path);

        FileInputStream is = new FileInputStream(file);

        JarEntry jarEntry = new JarEntry(path);
        os.putNextEntry(jarEntry);

        byte[] buf = new byte[4096];
        int read;

        while ((read = is.read(buf)) != -1) {
            os.write(buf, 0, read);
        }

        os.closeEntry();

        is.close();
    }

    public void delete() {
        delete(sourceDir.getPath(), sourceDir);
    }

    public void delete(String prefix, File file) {

        File files[] = file.listFiles();
        if (files != null) {
            for (int i=0; i<files.length; i++) {
                delete(prefix, files[i]);
            }
        }

        //String path = file.getPath();
        //System.out.println("Deleting "+path);

        file.delete();
    }

    public static void main(String args[]) throws Exception {

        if (args.length != 1) {
            System.out.println("Usage:");
            System.out.println("    schema.bat <file.schema>");
            System.out.println("    schema.sh <file.schema>");
            System.exit(0);
        }

        File file = new File(args[0]);
        file = file.getAbsoluteFile();

        System.out.print("Generating schema classes for "+file.getPath()+"...");
        System.out.flush();

        SchemaGenerator sg = new SchemaGenerator(file);
        sg.run();

        System.out.println("done.");
    }
}
