package org.safehaus.penrose.samba;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.event.BindEvent;
import org.safehaus.penrose.event.AddEvent;
import org.safehaus.penrose.event.ModifyEvent;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.ldap.*;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author Endi S. Dewata
 */
public class SambaUserModule extends Module {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static String SSH_CLIENT   = "ssh.client";
    public final static String SAMBA_ADMIN  = "samba.admin";
    public final static String SAMBA_SERVER = "samba.server";

    public void init() throws Exception {
        log.debug("Initializing SambaUserModule.");
        for (String name : getParameterNames()) {
            String value = getParameter(name);
            log.debug(" - " + name + ": " + value);
        }
    }

    public void afterBind(BindEvent event) throws Exception {

        BindResponse response = event.getResponse();
        int rc = response.getReturnCode();

        if (rc != LDAPException.SUCCESS) return;

        BindRequest bindRequest = event.getRequest();
        DN dn = bindRequest.getDn();
        log.debug("Checking NT Password and LM Password for "+dn+".");

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setDn(dn);
        searchRequest.setFilter("(objectClass=*)");
        searchRequest.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse<SearchResult> searchResponse = new SearchResponse<SearchResult>();

        Session session = event.getSession();
        session.search(searchRequest, searchResponse);

        SearchResult result = searchResponse.next();
        Attributes attributes = result.getAttributes();

        if (attributes.get("sambaNTPassword") == null ||
                attributes.get("sambaLMPassword") == null) {

            log.debug("Adding NT Password and LM Password.");

            Collection<Modification> modifications = new ArrayList<Modification>();

            Attribute attribute = new Attribute("userPassword", bindRequest.getPassword());
            Modification modification = new Modification(Modification.REPLACE, attribute);
            modifications.add(modification);

            session.modify(dn, modifications);

        } else {
            log.debug("NT Password and LM Password already exist.");
        }
    }

    public void beforeAdd(AddEvent event) throws Exception {
        AddRequest request = event.getRequest();

        String dn = request.getDn().toString();
        int i = dn.indexOf("=");
        int j = dn.indexOf(",", i);
        String username = dn.substring(i+1, j);

        log.debug("Checking Samba attributes before adding \""+dn+"\".");

        Attributes attributes = request.getAttributes();
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

            attributes.setValue("uidNumber", uid);
            attributes.setValue("gidNumber", gid);
            attributes.setValue("sambaSID", userSID);
            attributes.setValue("sambaPrimaryGroupSID", groupSID);
            attributes.setValue("sambaAcctFlags", flags);
        }
    }

    public void beforeModify(ModifyEvent event) throws Exception {

        ModifyRequest modifyRequest = event.getRequest();

        DN dn = modifyRequest.getDn();
        RDN rdn = dn.getRdn();
        String username = (String)rdn.get("uid");

        log.debug("Checking Samba attributes before modifying \""+dn+"\".");

        Session session = event.getSession();

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setDn(dn);
        searchRequest.setFilter("(objectClass=*)");
        searchRequest.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse<SearchResult> searchResponse = new SearchResponse<SearchResult>();

        session.search(searchRequest, searchResponse);

        SearchResult result = searchResponse.next();
        Attributes attributes = result.getAttributes();

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

            Collection<Modification> modifications = modifyRequest.getModifications();

            Attribute attribute = new Attribute("uidNumber", uid);
            Modification modification = new Modification(Modification.ADD, attribute);
            modifications.add(modification);

            attribute = new Attribute("gidNumber", gid);
            modification = new Modification(Modification.ADD, attribute);
            modifications.add(modification);

            attribute = new Attribute("sambaSID", userSID);
            modification = new Modification(Modification.ADD, attribute);
            modifications.add(modification);

            attribute = new Attribute("sambaPrimaryGroupSID", groupSID);
            modification = new Modification(Modification.ADD, attribute);
            modifications.add(modification);

            attribute = new Attribute("sambaAcctFlags", flags);
            modification = new Modification(Modification.ADD, attribute);
            modifications.add(modification);
        }
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

    public void addUser(String username) throws Exception {
        String client = getParameter(SSH_CLIENT);
        String admin  = getParameter(SAMBA_ADMIN);
        String server = getParameter(SAMBA_SERVER);

        Runtime rt = Runtime.getRuntime();
        String command = "/usr/sbin/useradd "+username;

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
        String command = "/bin/grep "+username+": /etc/passwd";

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
