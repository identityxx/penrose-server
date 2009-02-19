package org.safehaus.penrose.nis;

import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.util.TextUtil;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingEnumeration;
import javax.naming.Binding;
import java.util.Hashtable;
import java.util.Map;

public class NISJNDIClient extends NISClient {

    public Hashtable<String,String> parameters;

    public Context context;

    public NISJNDIClient() throws Exception {
    }

    public void init(Map<String,String> parameters) throws Exception {

        this.parameters = new Hashtable<String,String>();
        this.parameters.putAll(parameters);

        connect();
    }

    public void connect() throws Exception {
        context = new InitialContext(this.parameters);
    }

    public Context getContext() throws Exception {
        return context;
    }

    public void close() throws Exception {
        context.close();
    }

    public void lookup(
            String base,
            RDN rdn,
            String type,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String name;

        if ("ipService".equals(type)) {
            Object ipServicePort = rdn.get("ipServicePort");
            Object ipServiceProtocol = rdn.get("ipServiceProtocol");
            name = ipServicePort+"/"+ipServiceProtocol;

        } else {
            String attrName = rdn.getNames().iterator().next();
            name = rdn.get(attrName).toString();
        }

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("LOOKUP", 80));
            log.debug(TextUtil.displayLine(" - Base: "+base, 80));
            log.debug(TextUtil.displayLine(" - Name: "+name, 80));
            log.debug(TextUtil.displayLine(" - Type: "+type, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        try {
            Context baseContext = (Context)context.lookup("system/"+base);
            Object object = null;

            if (name.startsWith("/")) { // NIS provider bug

                if (debug) log.debug("Bindings:");
                NamingEnumeration ne = baseContext.listBindings("");
                while (ne.hasMore()) {
                    Binding binding = (Binding)ne.next();

                    if (debug) log.debug(" - "+binding.getName()+": "+binding.getObject());
                    if (!name.equals(binding.getName())) continue;

                    object = binding.getObject();
                    break;
                }

            } else {
                object = baseContext.lookup(name);
            }

            if (object == null) throw LDAP.createException(LDAP.NO_SUCH_OBJECT);

            SearchResult searchResult = createSearchResult(base, type, name, object.toString());
            if (searchResult == null) throw LDAP.createException(LDAP.NO_SUCH_OBJECT);

            if (debug) {
                searchResult.print();
            }

            response.add(searchResult);

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
            NamingEnumeration ne = context.listBindings("system/"+base);

            while (ne.hasMore()) {
                Binding binding = (Binding)ne.next();
                String name = binding.getName();
                Object object = binding.getObject();

                SearchResult searchResult = createSearchResult(base, type, name, object.toString());
                if (searchResult == null) continue;

                if (debug) {
                    searchResult.print();
                }

                response.add(searchResult);
            }

        } finally {
            response.close();
        }
    }
}
