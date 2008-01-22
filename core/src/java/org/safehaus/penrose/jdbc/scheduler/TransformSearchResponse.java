package org.safehaus.penrose.jdbc.scheduler;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.jdbc.connection.JDBCConnection;

/**
 * @author Endi Sukma Dewata
 */
public class TransformSearchResponse extends SearchResponse {

    Source source;
    SearchResponse response;

    String baseDn;

    public TransformSearchResponse(Source source, SearchResponse response) {
        this.source = source;
        this.response = response;

        baseDn = source.getParameter(JDBCConnection.BASE_DN);
    }

    public void add(SearchResult result) throws Exception {
        DN dn = result.getDn();
        RDN rdn = dn.getRdn();

        RDNBuilder rb = new RDNBuilder();

        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);

            int i = name.indexOf(".");
            name = name.substring(i+1);

            rb.set(name, value);
        }

        DNBuilder db = new DNBuilder();
        db.append(rb.toRdn());

        if (baseDn != null) {
            db.append(baseDn);
        }

        result.setDn(db.toDn());
        
        Attributes attributes = result.getAttributes();
        SourceValues sourceValues = result.getSourceValues();

        for (String sourceName : sourceValues.getNames()) {
            Attributes attrs = sourceValues.get(sourceName);
            attributes.add(attrs);
        }

        response.add(result);
    }

    public void close() throws Exception {
        response.close();
    }
}
