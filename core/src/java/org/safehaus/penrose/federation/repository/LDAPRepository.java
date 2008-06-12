package org.safehaus.penrose.federation.repository;

import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPRepository extends Repository {

    protected String protocol;
    protected String server;
    protected int port;

    public LDAPRepository() {
        setType("LDAP");
    }

    public LDAPRepository(Repository repository) {
        super(repository);
    }

    public String getUrl() {
        return parameters.get("url");
    }

    public void setUrl(String url) {
        parameters.put("url", url);
        parseUrl();
    }

    public void parseUrl() {
        String url = parameters.get("url");
        if (url == null) return;

        int i = url.indexOf("://");
        if (i < 0) throw new RuntimeException("Invalid URL: "+url);

        protocol = url.substring(0, i);

        int j = url.indexOf("/", i+3);
        if (j < 0) j = url.length();

        String s = url.substring(i+3, j);
        int k = s.indexOf(":");

        if (k < 0) {
            server = s;

            if ("ldap".equals(protocol)) {
                port = 389;

            } else if ("ldaps".equals(protocol)) {
                port = 636;

            } else {
                throw new RuntimeException("Unknown protocol: "+protocol);
            }

        } else {
            server = s.substring(0, k);
            port = Integer.parseInt(s.substring(k+1));
        }

        String suffix = url.substring(j+1);
        if (suffix != null && !"".equals(suffix)) {
            parameters.put("url", url.substring(0, j+1));
            parameters.put("suffix", suffix);
        }
    }

    public void updateUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append(protocol);
        sb.append("://");
        sb.append(server);
        if ("ldap".equals(protocol) && port != 389 || "ldaps".equals(protocol) && port != 636) {
            sb.append(":");
            sb.append(port);
        }
        sb.append("/");

        parameters.put("url", sb.toString());
    }

    public String getSuffix() {
        return parameters.get("suffix");
    }

    public void setSuffix(String suffix) {
        parameters.put("suffix", suffix);
    }

    public String getUser() {
        return parameters.get("user");
    }

    public void setUser(String user) {
        parameters.put("user", user);
    }

    public String getPassword() {
        return parameters.get("password");
    }

    public void setPassword(String password) {
        parameters.put("password", password);
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
        updateUrl();
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
        updateUrl();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
        updateUrl();
    }

    public void setParameters(Map<String,String> parameters) {
        super.setParameters(parameters);
        parseUrl();
    }
}
