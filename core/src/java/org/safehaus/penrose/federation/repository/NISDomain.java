package org.safehaus.penrose.federation.repository;

import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class NISDomain extends Repository {
    
    protected String fullName;
    protected String server;

    public NISDomain() {
        setType("NIS");
    }

    public NISDomain(Repository repository) {
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

        int i = url.indexOf("://");
        if (i < 0) throw new RuntimeException("Invalid URL: "+url);

        String protocol = url.substring(0, i);
        if (!"nis".equals(protocol)) throw new RuntimeException("Unknown protocol: "+protocol);

        int j = url.indexOf("/", i+3);
        if (j < 0) throw new RuntimeException("Missing NIS domain name.");

        server = url.substring(i+3, j);
        fullName = url.substring(j+1);
    }

    public void updateUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append("nis://");
        sb.append(server);
        sb.append("/");
        sb.append(fullName);

        parameters.put("url", sb.toString());
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
        updateUrl();
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
        updateUrl();
    }

    public boolean isNisEnabled() {
        return !"false".equals(parameters.get("nisEnabled"));
    }

    public void setNisEnabled(boolean nisEnabled) {
        if (nisEnabled) {
            parameters.remove("nisEnabled");
        } else {
            parameters.put("nisEnabled", "false");
        }
    }

    public String getNisSuffix() {
        return parameters.get("nisSuffix");
    }

    public void setNisSuffix(String nisSuffix) {
        parameters.put("nisSuffix", nisSuffix);
    }

    public String getDbSuffix() {
        return parameters.get("dbSuffix");
    }

    public void setDbSuffix(String dbSuffix) {
        parameters.put("dbSuffix", dbSuffix);
    }

    public boolean isYpEnabled() {
        return !"false".equals(parameters.get("ypEnabled"));
    }

    public void setYpEnabled(boolean ypEnabled) {
        if (ypEnabled) {
            parameters.remove("ypEnabled");
        } else {
            parameters.put("ypEnabled", "false");
        }
    }

    public String getYpSuffix() {
        return parameters.get("ypSuffix");
    }

    public void setYpSuffix(String ypSuffix) {
        parameters.put("ypSuffix", ypSuffix);
    }

    public boolean isNssEnabled() {
        return !"false".equals(parameters.get("nssEnabled"));
    }

    public void setNssEnabled(boolean nssEnabled) {
        if (nssEnabled) {
            parameters.remove("nssEnabled");
        } else {
            parameters.put("nssEnabled", "false");
        }
    }

    public String getNssSuffix() {
        return parameters.get("nssSuffix");
    }

    public void setNssSuffix(String nssSuffix) {
        parameters.put("nssSuffix", nssSuffix);
    }
    
    public void setParameters(Map<String,String> parameters) {
        super.setParameters(parameters);
        parseUrl();
    }
}
