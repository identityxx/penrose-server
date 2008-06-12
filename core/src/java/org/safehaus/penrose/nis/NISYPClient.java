package org.safehaus.penrose.nis;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.thread.ReaderThread;

import javax.naming.Context;
import java.util.Hashtable;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;

public class NISYPClient extends NISClient {

    public Hashtable<String,String> parameters;

    public String hostname;
    public String domain;

    public NISYPClient() throws Exception {
    }

    public void init(Map<String,String> parameters) throws Exception {

        this.parameters = new Hashtable<String,String>();
        this.parameters.putAll(parameters);

        String url = parameters.get(Context.PROVIDER_URL);

        int i = url.indexOf("://")+3;
        int j = url.indexOf("/", i);

        hostname = url.substring(i, j);
        domain = url.substring(j+1);
    }

    public void lookup(
            String base,
            RDN rdn,
            String type,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String attrName = rdn.getNames().iterator().next();
        String name = (String)rdn.get(attrName);

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("YPMATCH", 80));
            log.debug(TextUtil.displayLine(" - Map: "+base, 80));
            log.debug(TextUtil.displayLine(" - Key: "+name, 80));
            log.debug(TextUtil.displayLine(" - Type: "+type, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        BufferedReader in = null;
        PrintWriter out = null;

        try {
            Process process = ypmatch(name, base);

            in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            out = new PrintWriter(new OutputStreamWriter(process.getOutputStream()), true);

            new ReaderThread(process.getErrorStream(), System.err).start();

            String line = in.readLine();
            if (line == null) {
                throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
            }

            SearchResult searchResult = createSearchResult(base, type, null, line);
            if (searchResult == null) {
                throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
            }

            if (debug) {
                searchResult.print();
            }

            response.add(searchResult);

            in.close();

            int rc = process.waitFor();
            if (debug) log.debug("RC: "+rc);

            if (rc != 0) {
                throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
            }

        } finally {
            if (in != null) in.close();
            if (out != null) out.close();

            response.close();
        }
    }

    public void list(
            String base,
            String type,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("YPCAT", 80));
            log.debug(TextUtil.displayLine(" - Base: "+base, 80));
            log.debug(TextUtil.displayLine(" - Type: "+type, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        BufferedReader in = null;
        PrintWriter out = null;

        try {
            Process process = ypcat(base);

            in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            out = new PrintWriter(new OutputStreamWriter(process.getOutputStream()), true);

            new ReaderThread(process.getErrorStream(), System.err).start();

            String line;

            while ((line = in.readLine()) != null) {
                SearchResult searchResult = createSearchResult(base, type, null, line);
                if (searchResult == null) continue;

                if (debug) {
                    searchResult.print();
                }

                response.add(searchResult);
            }

            int rc = process.waitFor();
            if (debug) log.debug("RC: "+rc);

            if (rc != 0) {
                throw LDAP.createException(LDAP.OPERATIONS_ERROR);
            }

        } finally {
            if (in != null) in.close();
            if (out != null) out.close();

            response.close();
        }
    }

    public Process ypmatch(String key, String map) throws Exception {

        boolean debug = log.isDebugEnabled();

        String[] command = new String[] {
                "/usr/bin/ypmatch",
                "-k",
                "-d",
                domain,
                key,
                map
        };

        if (debug) {
            StringBuilder sb = new StringBuilder();
            for (int i=0; i<command.length; i++) {
                if (i > 0) sb.append(" ");
                sb.append(command[i]);
            }
            log.debug("Command: "+sb);
        }

        Runtime rt = Runtime.getRuntime();
        return rt.exec(command);
    }

    public Process ypcat(String map) throws Exception {

        boolean debug = log.isDebugEnabled();

        String[] command = new String[] {
                "/usr/bin/ypcat",
                "-k",
                "-d",
                domain,
                "-h",
                hostname,
                map
        };

        if (debug) {
            StringBuilder sb = new StringBuilder();
            for (int i=0; i<command.length; i++) {
                if (i > 0) sb.append(" ");
                sb.append(command[i]);
            }
            log.debug("Command: "+sb);
        }

        Runtime rt = Runtime.getRuntime();
        return rt.exec(command);
    }

    public DN parse(
            String base, String type,
            String name,
            String line,
            Attributes attributes
    ) throws Exception {

        // extract name from first column
        int i = line.indexOf(" ");
        if (i < 0) {
            name = line;
            line = "";
        } else {
            name = line.substring(0, i);
            line = line.substring(i+1).trim();
        }

        return super.parse(base, type, name, line, attributes);
    }
}
