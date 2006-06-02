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

import org.apache.directory.server.core.tools.schema.DirectorySchemaToolMojo;
import org.apache.directory.server.core.schema.bootstrap.AbstractBootstrapSchema;
import org.apache.maven.embedder.MavenEmbedder;
import org.apache.maven.embedder.MavenEmbedderConsoleLogger;
import org.apache.maven.embedder.PlexusLoggerAdapter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.monitor.event.EventMonitor;
import org.apache.maven.monitor.event.DefaultEventMonitor;
import org.apache.maven.cli.ConsoleDownloadMonitor;
import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.jar.JarEntry;
import java.util.*;
import java.lang.reflect.Method;
import java.lang.reflect.Field;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi S. Dewata
 */
public class SchemaGenerator {

    public static Logger log = Logger.getLogger(SchemaGenerator.class);

    File schemaDir;
    String name;

    File sourceDir;
    File output;

    private Collection dependencies;

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
        String pkg = "org.apache.directory.server.core.schema.bootstrap";

        int counter = 0;
        int size = dependencies == null ? 2 : dependencies.size() + 2;
        String deps[] = new String[size];
        deps[counter++] = "system";
        deps[counter++] = "core";

        if (dependencies != null) {
            for (Iterator i=dependencies.iterator(); i.hasNext(); ) {
                String dep = (String)i.next();
                deps[counter++] = dep;
            }
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        MavenEmbedder maven = new MavenEmbedder();

        maven.setClassLoader(classLoader);
        maven.setLogger(new MavenEmbedderConsoleLogger());
        maven.start();

        File homeDir = new File(System.getProperty("penrose.home"));
        File pomFile = new File(homeDir, "pom.xml");

        MavenProject pom = maven.readProjectWithDependencies(pomFile);

        org.apache.directory.server.core.tools.schema.Schema schemas[] = new org.apache.directory.server.core.tools.schema.Schema[1];
        schemas[0] = new org.apache.directory.server.core.tools.schema.Schema();
        schemas[0].setName(name);
        schemas[0].setDependencies(deps);
        schemas[0].setPkg(pkg);
        schemas[0].setOwner(owner);

        DirectorySchemaToolMojo tool = new DirectorySchemaToolMojo();

        Class clazz = tool.getClass();

        Field schemaField = clazz.getDeclaredField("schemas");
        schemaField.setAccessible(true);
        schemaField.set(tool, schemas);

        Field verboseOutputField = clazz.getDeclaredField("verboseOutput");
        verboseOutputField.setAccessible(true);
        verboseOutputField.setBoolean(tool, true);

        Field sourceDirectoryField = clazz.getDeclaredField("sourceDirectory");
        sourceDirectoryField.setAccessible(true);
        sourceDirectoryField.set(tool, schemaDir);

        Field outputDirectoryField = clazz.getDeclaredField("outputDirectory");
        outputDirectoryField.setAccessible(true);
        outputDirectoryField.set(tool, sourceDir);

        Field defaultPackageField = clazz.getDeclaredField("defaultPackage");
        defaultPackageField.setAccessible(true);
        defaultPackageField.set(tool, pkg);

        Field defaultOwnerField = clazz.getDeclaredField("defaultOwner");
        defaultOwnerField.setAccessible(true);
        defaultOwnerField.set(tool, owner);

        Field projectField = clazz.getDeclaredField("project");
        projectField.setAccessible(true);
        projectField.set(tool, pom);

        tool.execute();
/*
        EventMonitor eventMonitor = new DefaultEventMonitor(
                new PlexusLoggerAdapter(new MavenEmbedderConsoleLogger())
        );

        maven.execute(
                pom,
                Collections.singletonList("package"),
                eventMonitor,
                new ConsoleDownloadMonitor(),
                null,
                targetDirectory
        );
*/
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

        String paths[] = new String[] { file.getAbsolutePath() };
        //Main.compile(paths);

        Class clazz = Class.forName("com.sun.tools.javac.Main");
        Method method = clazz.getMethod("compile", new Class[] { Class.forName("[Ljava.lang.String;") });

        method.invoke(null, new Object[] { paths });
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

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.schema.SchemaGenerator [OPTION]... <FILE>");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -?, --help         display this help and exit");
        System.out.println("  -n                 generate source code only");
    }

    public static void main(String args[]) throws Exception {

        boolean generateOnly = false;

        StringBuffer depends = new StringBuffer();

        LongOpt[] longopts = new LongOpt[1];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, '?');

        Getopt getopt = new Getopt("SchemaGenerator", args, "-:?d:n", longopts);

        Collection parameters = new ArrayList();
        int c;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case ':':
                case '?':
                    showUsage();
                    System.exit(0);
                    break;
                case 1:
                    parameters.add(getopt.getOptarg());
                    break;
                case 'd':
                    depends.append(getopt.getOptarg());
                    break;
                case 'n':
                    generateOnly = true;
                    break;
            }
        }

        if (parameters.size() == 0) {
            showUsage();
            System.exit(0);
        }

        String homeDirectory = System.getProperty("penrose.home");

        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        File log4jProperties = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.properties");
        File log4jXml = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.xml");

        if (log4jProperties.exists()) {
            PropertyConfigurator.configure(log4jProperties.getAbsolutePath());

        } else if (log4jXml.exists()) {
            DOMConfigurator.configure(log4jXml.getAbsolutePath());

        } else {
            logger.setLevel(Level.DEBUG);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
            BasicConfigurator.configure(appender);
        }

        Iterator iterator = parameters.iterator();
        String path = (String)iterator.next();

        File file = new File(path);
        file = file.getAbsoluteFile();

        SchemaGenerator sg = new SchemaGenerator(file);

        if (depends.length() > 0) {
            sg.setDependencies(Arrays.asList(depends.toString().split(" ")));
        }

        if (generateOnly) {
            sg.generate();

        } else {
            sg.run();
        }
    }

    public Collection getDependencies() {
        return dependencies;
    }

    public void setDependencies(Collection dependencies) {
        this.dependencies = dependencies;
    }
}
