package org.safehaus.penrose.server;

import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.config.PenroseConfigReader;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.config.PenroseConfigWriter;
import org.safehaus.penrose.service.*;
import org.safehaus.penrose.ldap.DN;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;

import java.io.*;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Arrays;

/**
 * @author Endi Sukma Dewata
 */
public class PenroseServerConfigurator {

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    PrintWriter out = new PrintWriter(System.out, true);
    PrintWriter err = new PrintWriter(System.err, true);

    PenroseConfigReader penroseReader = new PenroseConfigReader();
    ServiceReader serviceReader = new ServiceReader();

    PenroseConfig penroseConfig;
    File home;

    public PenroseServerConfigurator(File home) throws Exception {
        this.home = home;
    }

    public void run() throws Exception {

        String title = "Configuring "+PenroseServer.PRODUCT_NAME+":";

        out.println(title);
        out.println(TextUtil.repeat("-", title.length()));
        out.println();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Penrose Server
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        File serverXml = new File(home, "conf"+File.separator+"server.xml");
        PenroseConfig penroseConfig = penroseReader.read(serverXml);

        String hostname = penroseConfig.getSystemProperty("java.rmi.server.hostname");
        if (hostname == null || "".equals(hostname) || "localhost".equals(hostname)) {
            hostname = InetAddress.getLocalHost().getHostName();
        }
        out.print("Hostname ["+hostname+"]: ");
        out.flush();

        String s = in.readLine();
        if (s != null) {
            s = s.trim();
            if (!"".equals(s)) hostname = s;
        }

        penroseConfig.setSystemProperty("java.rmi.server.hostname", hostname);

        DN rootDn = penroseConfig.getRootDn();
        if (rootDn == null) {
            rootDn = new DN("uid=admin,ou=system");
        }
        out.print("Root DN ["+rootDn+"]: ");
        out.flush();

        s = in.readLine();
        if (s != null) {
            s = s.trim();
            if (!"".equals(s)) rootDn = new DN(s);
        }

        penroseConfig.setRootDn(rootDn);

        byte rootPassword[] = penroseConfig.getRootPassword();
        out.print("Root Password [*****]: ");
        out.flush();

        s = in.readLine();
        if (s != null) {
            s = s.trim();
            if (!"".equals(s)) rootPassword = s.getBytes();
        }

        penroseConfig.setRootPassword(rootPassword);

        String osName = System.getProperty("os.name");
        if ("Linux".equalsIgnoreCase(osName)) {

            String owner[] = getOwner();

            String user = owner[0];
            String group = owner[1];

            out.print("User account ["+user+"]: ");
            out.flush();

            s = in.readLine();
            if (s != null) {
                s = s.trim();
                if (!"".equals(s)) user = s;
            }

            out.print("Group account ["+group+"]: ");
            out.flush();

            s = in.readLine();
            if (s != null) {
                s = s.trim();
                if (!"".equals(s)) group = s;
            }

            setOwner(user, group);
            setPermission();
        }

        PenroseConfigWriter penroseWriter = new PenroseConfigWriter();
        penroseWriter.write(serverXml, penroseConfig);

        out.println();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Penrose Services
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        File servicesDir = new File(home, "services");
        ServiceConfigManager serviceConfigManager = new ServiceConfigManager(servicesDir);

        for (String serviceName : serviceConfigManager.getAvailableServiceNames()) {

            ServiceConfig serviceConfig = serviceConfigManager.load(serviceName);

            File serviceDir = new File(servicesDir, serviceName);
            ClassLoader classLoader = new ServiceClassLoader(serviceDir, getClass().getClassLoader());

            String serviceClass = serviceConfig.getServiceClass();
            String serviceConfiguratorClass = serviceClass+"Configurator";

            //Collection<URL> classPaths = serviceConfig.getClassPaths();
            //URLClassLoader classLoader = new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]), getClass().getClassLoader());

            try {
                Class clazz = classLoader.loadClass(serviceConfiguratorClass);

                title = "Configuring "+serviceName+" Service:";

                out.println(title);
                out.println(TextUtil.repeat("-", title.length()));
                out.println();

                ServiceConfigurator serviceConfigurator = (ServiceConfigurator)clazz.newInstance();

                serviceConfigurator.setServiceDir(serviceDir);
                serviceConfigurator.setServiceConfig(serviceConfig);

                serviceConfigurator.init();
                serviceConfigurator.configure();
                serviceConfigurator.close();

            } catch (ClassNotFoundException e) {
                // ignore
            }

            serviceConfigManager.store(serviceName, serviceConfig);

            out.println();
        }
    }

    public static void main(String args[]) throws Exception {

        Collection parameters = Arrays.asList(args);

        if (parameters.contains("-?") || parameters.contains("--help")) {
            System.out.println("Usage: org.safehaus.penrose.server.PenroseServerConfigurator [OPTION]...");
            System.out.println();
            System.out.println("  -?, --help     display this help and exit");
            System.out.println("  -d             run in debug mode");
            System.out.println("  -v             run in verbose mode");
            System.out.println("      --version  output version information and exit");
            System.exit(0);
        }

        if (parameters.contains("--version")) {
            System.out.println(PenroseServer.PRODUCT_NAME+" Configurator "+PenroseServer.PRODUCT_VERSION);
            System.exit(0);
        }

        File home = new File(System.getProperty("penrose.home"));

        File log4jXml = new File(home, "conf"+File.separator+"log4j.xml");

        if (log4jXml.exists()) {
            DOMConfigurator.configure(log4jXml.getAbsolutePath());
        }

        for (Enumeration e = Logger.getRootLogger().getLoggerRepository().getCurrentLoggers(); e.hasMoreElements(); ) {
            Logger logger = (Logger)e.nextElement();

            if (parameters.contains("-d")) {
                if (logger.getAppender("console") != null) {
                    logger.setLevel(Level.DEBUG);
                    logger.removeAppender("console");
                    ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
                    logger.addAppender(appender);
                }

            } else if (parameters.contains("-v")) {
                if (logger.getAppender("console") != null) {
                    logger.setLevel(Level.INFO);
                    logger.removeAppender("console");
                    ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss.SSS}] %m%n"));
                    logger.addAppender(appender);
                }
            }
        }

        PenroseServerConfigurator app = new PenroseServerConfigurator(home);
        app.run();
    }

    public String[] getOwner() throws Exception {

        Runtime rt = Runtime.getRuntime();
        final Process p = rt.exec(new String[] {
                "/bin/ls",
                "-ld",
                home.getAbsolutePath()
        });

        new Thread() {
            public void run() {
                BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                String line;
                try {
                    while ((line = error.readLine()) != null) err.println(line);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = input.readLine();

        String s[] = line.split(" +");

        String owner[] = new String[2];
        owner[0] = s[2];
        owner[1] = s[3];

        return owner;
    }

    public void setOwner(String user, String group) throws Exception {

        Runtime rt = Runtime.getRuntime();
        final Process p = rt.exec(new String[] {
                "/bin/chown",
                "-R",
                user+"."+group,
                home.getAbsolutePath()
        });

        new Thread() {
            public void run() {
                BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                String line;
                try {
                    while ((line = error.readLine()) != null) err.println(line);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            public void run() {
                BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;
                try {
                    while ((line = input.readLine()) != null) out.println(line);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        p.waitFor();
    }

    public void setPermission() throws Exception {

        Runtime rt = Runtime.getRuntime();
        final Process p = rt.exec(new String[] {
                "/bin/chmod",
                "-R",
                "o-rwx",
                home.getAbsolutePath()
        });

        new Thread() {
            public void run() {
                BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                String line;
                try {
                    while ((line = error.readLine()) != null) err.println(line);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            public void run() {
                BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;
                try {
                    while ((line = input.readLine()) != null) out.println(line);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        p.waitFor();
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public File getHome() {
        return home;
    }

    public void setHome(File home) {
        this.home = home;
    }
}
