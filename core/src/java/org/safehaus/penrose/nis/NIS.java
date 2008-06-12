package org.safehaus.penrose.nis;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author Endi Sukma Dewata
 */
public class NIS {

    public final static String JNDI           = "jndi";
    public final static String YP             = "yp";
    public final static String LOCAL          = "local";

    public final static String METHOD         = "method";
    public final static String DEFAULT_METHOD = JNDI;

    public final static String LOOKUP         = "lookup";
    public final static String LIST           = "list";

    public final static String BASE           = "base";
    public final static String SCOPE          = "scope";
    public final static String FILTER         = "filter";
    public final static String OBJECT_CLASSES = "objectClasses";
    public final static String PAM            = "pam";

    public static Map<String,String> mapLabels = new TreeMap<String,String>();

    static {
        mapLabels.put("users",      "Users");
        mapLabels.put("groups",     "Groups");
        mapLabels.put("hosts",      "Hosts");
        mapLabels.put("services",   "Services");
        mapLabels.put("rpcs",       "RPCs");
        mapLabels.put("netids",     "NetIDs");
        mapLabels.put("protocols",  "Protocols");
        mapLabels.put("aliases",    "Aliases");
        mapLabels.put("netgroups",  "Netgroups");
        mapLabels.put("ethers",     "Ethers");
        mapLabels.put("bootparams", "BootParams");
        mapLabels.put("networks",   "Networks");
        mapLabels.put("automounts", "Automounts");
    }
}
