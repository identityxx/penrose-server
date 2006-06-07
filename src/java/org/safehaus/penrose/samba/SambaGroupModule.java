package org.safehaus.penrose.samba;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.AddEvent;
import org.safehaus.penrose.event.ModifyEvent;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.apache.log4j.Logger;

import javax.naming.directory.*;
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

    Logger log = Logger.getLogger(getClass());

    public final static String SSH_CLIENT   = "ssh.client";
    public final static String SAMBA_SERVER = "samba.server";

    public void init() throws Exception {
        log.debug("Initializing SambaGroupModule.");
        for (Iterator i=getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = getParameter(name);
            log.debug(" - "+name+": "+value);
        }
    }

    public void beforeAdd(AddEvent event) throws Exception {
        Attributes attributes = event.getAttributes();

        String dn = event.getDn();
        int i = dn.indexOf("=");
        int j = dn.indexOf(",", i);
        String groupname = dn.substring(i+1, j);

        log.debug("Checking Samba attributes before adding \""+dn+"\".");

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

            Attribute attribute = new BasicAttribute("gidNumber", gid);
            attributes.put(attribute);

            attribute = new BasicAttribute("sambaSID", groupSID);
            attributes.put(attribute);
        }

        if (attributes.get("sambaGroupType") == null) {
            Attribute attribute = new BasicAttribute("sambaGroupType", "2");
            attributes.put(attribute);
        }
    }

    public void beforeModify(ModifyEvent event) throws Exception {

        String dn = event.getDn();
        int i = dn.indexOf("=");
        int j = dn.indexOf(",", i);
        String groupname = dn.substring(i+1, j);

        log.debug("Checking Samba attributes before modifying \""+dn+"\".");

        PenroseSession session = event.getSession();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        PenroseSearchResults results = session.search(
                dn,
                "(objectClass=*)",
                sc);

        SearchResult entry = (SearchResult)results.next();
        Attributes values = entry.getAttributes();

        Collection modifications = event.getModifications();

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

            Attribute attribute = new BasicAttribute("gidNumber", gid);
            ModificationItem modification = new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute);
            modifications.add(modification);

            attribute = new BasicAttribute("sambaSID", groupSID);
            modification = new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute);
            modifications.add(modification);
        }

        if (values.get("sambaGroupType") == null) {
            Attribute attribute = new BasicAttribute("sambaGroupType", "2");
            ModificationItem modification = new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute);
            modifications.add(modification);
        }
    }

    public Map getServerInfo() throws Exception {
        String client = getParameter(SSH_CLIENT);
        String server = getParameter(SAMBA_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = client+" root@"+server+" /usr/bin/net getlocalsid";
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
        String server = getParameter(SAMBA_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = client+" root@"+server+" /usr/sbin/groupadd "+groupname;
        log.debug(command);
        Process p = rt.exec(command);
        p.waitFor();
    }

    public Map getUserInfo(String username) throws Exception {
        String client = getParameter(SSH_CLIENT);
        String server = getParameter(SAMBA_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = client+" root@"+server+" /bin/grep "+username+": /etc/group";
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
