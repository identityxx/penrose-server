package org.safehaus.penrose.samba;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.AddEvent;
import org.safehaus.penrose.event.ModifyEvent;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.entry.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Iterator;
import java.io.InputStreamReader;
import java.io.BufferedReader;

/**
 * @author Endi S. Dewata
 */
public class SambaGroupModule extends Module {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String SSH_CLIENT   = "ssh.client";
    public final static String SAMBA_ADMIN  = "samba.admin";
    public final static String SAMBA_SERVER = "samba.server";

    public void init() throws Exception {
        log.debug("Initializing SambaGroupModule.");
        for (Iterator i=getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = getParameter(name);
            log.debug(" - "+name+": "+value);
        }
    }

    public boolean beforeAdd(AddEvent event) throws Exception {
        AddRequest request = event.getRequest();

        String dn = request.getDn().toString();
        int i = dn.indexOf("=");
        int j = dn.indexOf(",", i);
        String groupname = dn.substring(i+1, j);

        log.debug("Checking Samba attributes before adding \""+dn+"\".");

        Attributes attributes = request.getAttributes();
        if (attributes.get("sambaSID") == null || attributes.get("gidNumber") == null) {

            log.debug("Generating Group SID and GID ...");

            Map serverInfo = getServerInfo();
            String serverSID = (String)serverInfo.get("sid");

            String gid;
            String groupSID;

            if ("Domain Admins".equals(groupname)) {
                gid = "512";
                groupSID = serverSID+"-512";

            } else if ("Domain Users".equals(groupname)) {
                gid = "513";
                groupSID = serverSID+"-513";

            } else if ("Domain Guests".equals(groupname)) {
                gid = "514";
                groupSID = serverSID+"-514";

            } else {
                Map userInfo = getUserInfo(groupname);
                if (userInfo == null) {
                    addGroup(groupname);
                    userInfo = getUserInfo(groupname);
                }

                gid = (String)userInfo.get("gid");
                int v = gid == null ? 0 : Integer.parseInt(gid);
                groupSID = serverSID+"-"+(v * 2 + 1000);
            }

            log.debug("Group SID: "+groupSID);
            log.debug("GID: "+gid);

            attributes.setValue("gidNumber", gid);

            attributes.setValue("sambaSID", groupSID);
        }

        if (attributes.get("sambaGroupType") == null) {
            attributes.setValue("sambaGroupType", "2");
        }

        return true;
    }

    public boolean beforeModify(ModifyEvent event) throws Exception {

        ModifyRequest modifyRequest = event.getRequest();

        DN dn = modifyRequest.getDn();
        RDN rdn = dn.getRdn();
        String groupname = (String)rdn.get("cn");

        log.debug("Checking Samba attributes before modifying \""+dn+"\".");

        Session session = event.getSession();

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setDn(dn);
        searchRequest.setFilter("(objectClass=*)");
        searchRequest.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        session.search(searchRequest, response);

        SearchResult result = (SearchResult) response.next();
        Entry entry = result.getEntry();
        Attributes values = entry.getAttributes();

        Collection modifications = modifyRequest.getModifications();

        if (values.get("sambaSID") == null || values.get("gidNumber") == null) {

            log.debug("Generating Group SID and GID ...");

            Map serverInfo = getServerInfo();
            String serverSID = (String)serverInfo.get("sid");

            String gid;
            String groupSID;

            if ("Domain Admins".equals(groupname)) {
                gid = "512";
                groupSID = serverSID+"-512";

            } else if ("Domain Users".equals(groupname)) {
                gid = "513";
                groupSID = serverSID+"-513";

            } else if ("Domain Guests".equals(groupname)) {
                gid = "514";
                groupSID = serverSID+"-514";

            } else {
                Map userInfo = getUserInfo(groupname);
                if (userInfo == null) {
                    addGroup(groupname);
                    userInfo = getUserInfo(groupname);
                }

                gid = (String)userInfo.get("gid");
                int v = gid == null ? 0 : Integer.parseInt(gid);
                groupSID = serverSID+"-"+(v * 2 + 1000);
            }

            log.debug("Group SID: "+groupSID);
            log.debug("GID: "+gid);

            Attribute attribute = new Attribute("gidNumber", gid);
            Modification modification = new Modification(Modification.ADD, attribute);
            modifications.add(modification);

            attribute = new Attribute("sambaSID", groupSID);
            modification = new Modification(Modification.ADD, attribute);
            modifications.add(modification);
        }

        if (values.get("sambaGroupType") == null) {
            Attribute attribute = new Attribute("sambaGroupType", "2");
            Modification modification = new Modification(Modification.ADD, attribute);
            modifications.add(modification);
        }

        return true;
    }

    public Map getServerInfo() throws Exception {
        String client = getParameter(SSH_CLIENT);
        String admin  = getParameter(SAMBA_ADMIN);
        String server = getParameter(SAMBA_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = "/usr/bin/net getlocalsid";

        if (client != null && admin != null && server != null) {
            command = client+" "+admin +"@"+server+" "+command;
        }

        log.debug(command);
        Process p = rt.exec(command);

        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        p.waitFor();

        String line = in.readLine();
        log.debug("Response: "+line);

        if (line == null) return null;

        String text1 = "SID for domain ";
        String text2 = " is: ";

        int i = line.indexOf(text2, text1.length());
        String domain = line.substring(text1.length(), i);
        String sid = line.substring(i + text2.length());

        log.debug("Domain: "+domain);
        log.debug("SID   : "+sid);

        Map map = new TreeMap();
        map.put("domain", domain);
        map.put("sid", sid);

        return map;
    }

    public void addGroup(String groupname) throws Exception {
        String client = getParameter(SSH_CLIENT);
        String admin  = getParameter(SAMBA_ADMIN);
        String server = getParameter(SAMBA_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = "/usr/sbin/groupadd "+groupname;

        if (client != null && admin != null && server != null) {
            command = client+" "+admin +"@"+server+" "+command;
        }

        log.debug(command);
        Process p = rt.exec(command);
        p.waitFor();
    }

    public Map getUserInfo(String username) throws Exception {
        String client = getParameter(SSH_CLIENT);
        String admin  = getParameter(SAMBA_ADMIN);
        String server = getParameter(SAMBA_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = "/bin/grep "+username+": /etc/group";

        if (client != null && admin != null && server != null) {
            command = client+" "+admin +"@"+server+" "+command;
        }

        log.debug(command);
        Process p = rt.exec(command);

        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        p.waitFor();

        String line = in.readLine();
        log.debug("Response: "+line);

        if (line == null) return null;

        int i = line.indexOf(":");
        i = line.indexOf(":", i+1);
        int j = line.indexOf(":", i+1);
        String gid = line.substring(i+1, j);

        log.debug("GID: "+gid);

        Map map = new TreeMap();
        map.put("gid", gid);

        return map;
    }
}
