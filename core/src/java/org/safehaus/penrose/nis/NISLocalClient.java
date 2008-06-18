package org.safehaus.penrose.nis;

import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.util.TextUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Hashtable;
import java.util.Map;

public class NISLocalClient extends NISClient {

    public Hashtable<String,String> parameters;
    public File dir;

    public NISLocalClient() throws Exception {
    }

    public void init(Map<String,String> parameters) throws Exception {

        this.parameters = new Hashtable<String,String>();
        this.parameters.putAll(parameters);

        String s = parameters.get("dir");
        if (s != null) dir = new File(s);
    }

    public void lookup(
            String base,
            RDN rdn,
            String type,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("LOOKUP", 80));
            log.debug(TextUtil.displayLine(" - Base: "+base, 80));
            log.debug(TextUtil.displayLine(" - Name: "+rdn, 80));
            log.debug(TextUtil.displayLine(" - Type: "+type, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        try {
            File file = dir == null ? new File(base) : new File(dir, base);

            BufferedReader in = new BufferedReader(new FileReader(file));
            String line;

            if (debug) log.debug("Records:");
            while ((line = in.readLine()) != null) {
                if (debug) log.debug(" - "+line);
                
                SearchResult searchResult = createSearchResult(base, type, null, line);
                if (searchResult == null) continue;

                if (!rdn.equals(searchResult.getDn().getRdn())) continue;

                if (debug) {
                    searchResult.print();
                }

                response.add(searchResult);
                break;
            }

            in.close();

            if (line == null) {
                throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
            }

        } finally {
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
            log.debug(TextUtil.displayLine("LIST", 80));
            log.debug(TextUtil.displayLine(" - Base: "+base, 80));
            log.debug(TextUtil.displayLine(" - Type: "+type, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        try {
            File file = dir == null ? new File(base) : new File(dir, base);

            BufferedReader in = new BufferedReader(new FileReader(file));
            String line;

            while ((line = in.readLine()) != null) {
                SearchResult searchResult = createSearchResult(base, type, null, line);
                if (searchResult == null) continue;

                if (debug) {
                    searchResult.print();
                }

                response.add(searchResult);
            }

            in.close();

        } finally {
            response.close();
        }
    }
}
