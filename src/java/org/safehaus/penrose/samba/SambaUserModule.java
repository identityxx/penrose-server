package org.safehaus.penrose.samba;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.BindEvent;
import org.safehaus.penrose.event.AddEvent;
import org.safehaus.penrose.event.ModifyEvent;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.ietf.ldap.LDAPException;
import org.apache.log4j.Logger;

import javax.naming.directory.*;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author Endi S. Dewata
 */
public class SambaUserModule extends Module {

    Logger log = Logger.getLogger(getClass());

    public final static String SSH_CLIENT = "ssh.client";
    public final static String SSH_SERVER  = "ssh.server";

    public void init() throws Exception {
        log.debug("Initializing SambaUserModule.");
        for (Iterator i=getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = getParameter(name);
            log.debug(" - "+name+": "+value);
        }
    }

    public void afterBind(BindEvent event) throws Exception {

        if (event.getReturnCode() != LDAPException.SUCCESS) return;

        String dn = event.getDn();
        log.debug("Checking NT Password and LM Password for "+dn+".");

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        PenroseSession session = event.getSession();
        PenroseSearchResults results = session.search(
                dn,
                "(objectClass=*)",
                sc);

        SearchResult entry = (SearchResult)results.next();
        Attributes attributes = entry.getAttributes();

        if (attributes.get("sambaNTPassword") == null ||
                attributes.get("sambaLMPassword") == null) {

            log.debug("Adding NT Password and LM Password.");

            Collection modifications = new ArrayList();

            Attribute attribute = new BasicAttribute("userPassword", event.getPassword());

            ModificationItem modification = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute);
            modifications.add(modification);

            session.modify(dn, modifications);

        } else {
            log.debug("NT Password and LM Password already exist.");
        }
    }

    public void beforeAdd(AddEvent event) throws Exception {
        Attributes attributes = event.getAttributes();

        String dn = event.getDn();
        int i = dn.indexOf("=");
        int j = dn.indexOf(",", i);
        String username = dn.substring(i+1, j);

        log.debug("Checking Samba attributes before adding \""+dn+"\".");

        if (attributes.get("uidNumber") == null ||
                attributes.get("gidNumber") == null ||
                attributes.get("sambaSID") == null ||
                attributes.get("sambaPrimaryGroupSID") == null ||
                attributes.get("sambaAcctFlags") == null) {

            log.debug("Generating UID, GID, User SID, Group SID, and Flags.");

            Map serverInfo = getServerInfo();
            String serverSID = (String)serverInfo.get("sid");

            String uid;
            String gid;
            String userSID;
            String groupSID;
            String flags;

            if ("root".equals(username)) {
                uid = "0";
                gid = "0";
                userSID = serverSID+"-500";
                groupSID = serverSID+"-512";
                flags = "[U          ]";

            } else if ("nobody".equals(username)) {
                uid = "99";
                gid = "99";
                userSID = serverSID+"-501";
                groupSID = serverSID+"-514";
                flags = "[UX         ]";

            } else {
                Map userInfo = getUserInfo(username);
                if (userInfo == null) {
                    addUser(username);
                    userInfo = getUserInfo(username);
                }

                uid = (String)userInfo.get("uid");
                gid = (String)userInfo.get("gid");
                int v = uid == null ? 0 : Integer.parseInt(uid);
                int w = gid == null ? 0 : Integer.parseInt(gid);
                userSID = serverSID+"-"+(v * 2 + 1000);
                groupSID = serverSID+"-"+(w * 2 + 1001);
                flags = "[U          ]";
            }

            log.debug("Add Samba attributes to \""+dn+"\".");

            log.debug(" - UID       : "+uid);
            log.debug(" - GID       : "+gid);
            log.debug(" - User SID  : "+userSID);
            log.debug(" - Group SID : "+groupSID);
            log.debug(" - Flags     : "+flags);

            Attribute set = new BasicAttribute("uidNumber", uid);
            attributes.put(set);

            set = new BasicAttribute("gidNumber", gid);
            attributes.put(set);

            set = new BasicAttribute("sambaSID", userSID);
            attributes.put(set);

            set = new BasicAttribute("sambaPrimaryGroupSID", groupSID);
            attributes.put(set);

            set = new BasicAttribute("sambaAcctFlags", flags);
            attributes.put(set);
        }
    }

    public void beforeModify(ModifyEvent event) throws Exception {

        String dn = event.getDn();
        int i = dn.indexOf("=");
        int j = dn.indexOf(",", i);
        String username = dn.substring(i+1, j);

        log.debug("Checking Samba attributes before modifying \""+dn+"\".");

        PenroseSession session = event.getSession();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        PenroseSearchResults results = session.search(
                dn,
                "(objectClass=*)",
                sc);

        SearchResult entry = (SearchResult)results.next();
        Attributes attributes = entry.getAttributes();

        if (attributes.get("uidNumber") == null ||
                attributes.get("gidNumber") == null ||
                attributes.get("sambaSID") == null ||
                attributes.get("sambaPrimaryGroupSID") == null ||
                attributes.get("sambaAcctFlags") == null) {

            log.debug("Generating UID, GID, User SID, Group SID, and Flags.");

            Map serverInfo = getServerInfo();
            String serverSID = (String)serverInfo.get("sid");

            String uid;
            String gid;
            String userSID;
            String groupSID;
            String flags;

            if ("root".equals(username)) {
                uid = "0";
                gid = "0";
                userSID = serverSID+"-500";
                groupSID = serverSID+"-512";
                flags = "[U          ]";

            } else if ("nobody".equals(username)) {
                uid = "99";
                gid = "99";
                userSID = serverSID+"-501";
                groupSID = serverSID+"-514";
                flags = "[UX         ]";

            } else {
                Map userInfo = getUserInfo(username);
                if (userInfo == null) {
                    addUser(username);
                    userInfo = getUserInfo(username);
                }

                uid = (String)userInfo.get("uid");
                gid = (String)userInfo.get("gid");
                int v = uid == null ? 0 : Integer.parseInt(uid);
                int w = gid == null ? 0 : Integer.parseInt(gid);
                userSID = serverSID+"-"+(v * 2 + 1000);
                groupSID = serverSID+"-"+(w * 2 + 1001);
                flags = "[U          ]";
            }


            log.debug("Add Samba attributes to \""+dn+"\".");

            log.debug(" - UID       : "+uid);
            log.debug(" - GID       : "+gid);
            log.debug(" - User SID  : "+userSID);
            log.debug(" - Group SID : "+groupSID);
            log.debug(" - Flags     : "+flags);

            Collection modifications = event.getModifications();

            Attribute attribute = new BasicAttribute("uidNumber", uid);
            ModificationItem modification = new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute);
            modifications.add(modification);

            attribute = new BasicAttribute("gidNumber", gid);
            modification = new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute);
            modifications.add(modification);

            attribute = new BasicAttribute("sambaSID", userSID);
            modification = new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute);
            modifications.add(modification);

            attribute = new BasicAttribute("sambaPrimaryGroupSID", groupSID);
            modification = new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute);
            modifications.add(modification);

            attribute = new BasicAttribute("sambaAcctFlags", flags);
            modification = new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute);
            modifications.add(modification);
        }
    }

    public Map getServerInfo() throws Exception {
        String ssh = getParameter(SSH_CLIENT);
        String server = getParameter(SSH_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = ssh+" root@"+server+" /usr/bin/net getlocalsid";
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

    public void addUser(String username) throws Exception {
        String client = getParameter(SSH_CLIENT);
        String server = getParameter(SSH_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = client+" root@"+server+" /usr/sbin/useradd "+username;
        log.debug(command);
        Process p = rt.exec(command);
        p.waitFor();
    }

    public Map getUserInfo(String username) throws Exception {
        String client = getParameter(SSH_CLIENT);
        String server = getParameter(SSH_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = client+" root@"+server+" /bin/grep "+username+": /etc/passwd";
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
        String uid = line.substring(i+1, j);

        i = line.indexOf(":", j+1);
        String gid = line.substring(j+1, i);

        //log.debug("UID: "+uid);
        //log.debug("GID: "+gid);

        Map map = new TreeMap();
        map.put("uid", uid);
        map.put("gid", gid);

        return map;
    }
}
