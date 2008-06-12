/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.nis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.util.TextUtil;

import java.util.*;

import net.sf.jpam.Pam;
import net.sf.jpam.PamReturnValue;
import com.identyx.license.License;
import com.identyx.license.LicenseManager;

public abstract class NISClient {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Hashtable<String,String> parameters;

    public NISClient() throws Exception {

        LicenseManager licenseManager = LicenseManager.getInstance();
        License license = licenseManager.getLicense("Penrose Server");
        if (license == null) {
            throw new Exception("Invalid license.");
        }

        boolean valid = licenseManager.isValid(license);
        if (!valid) {
            throw new Exception("Invalid license.");
        }

        Date today = new Date();
        Date expiryDate = license.getExpiryDate();
        if (!today.before(expiryDate)) {
            throw new Exception("Expired license: "+ License.DATE_FORMAT.format(expiryDate));
        }
/*
        PenroseFactory factory = PenroseProFactory.getInstance();

        if (!(factory instanceof PenroseProFactory)) {
            throw new Exception("Invalid license.");
        }

        PenroseProFactory proFactory = (PenroseProFactory)factory;
        if (!proFactory.checkLicense()) {
            throw new Exception("Invalid license.");
        }
*/
    }

    public void init(Map<String,String> parameters) throws Exception {

        this.parameters = new Hashtable<String,String>();
        this.parameters.putAll(parameters);
    }

    public void close() throws Exception {
    }

    public void bind(String serviceName, String username, byte[] password) throws Exception {
        bind(serviceName, username, new String(password));
    }

    public void bind(String serviceName, String username, String password) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("Bind", 80));
            log.debug(TextUtil.displayLine(" - User    : "+username, 80));
            log.debug(TextUtil.displayLine(" - Password: "+password, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        try {
            Pam pam = new Pam(serviceName);
            PamReturnValue returnValue = pam.authenticate(username, password);

            if (!returnValue.equals(PamReturnValue.PAM_SUCCESS)) {
                log.error("PAM return value: "+returnValue);
                throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw LDAP.createException(e);
        }
    }

    public abstract void lookup(
            String base,
            RDN rdn,
            String type,
            SearchResponse response
    ) throws Exception;

    public abstract void list(
            String base,
            String type,
            SearchResponse response
    ) throws Exception;

    public SearchResult createSearchResult(
            String base,
            String type,
            String name,
            String line
    ) throws Exception {

        line = line.trim();
        if ("".equals(line) || line.startsWith("#")) {
            log.debug("Empty/comment line: "+line);
            return null;
        }

        Attributes attributes = new Attributes();
        DN dn = parse(base, type, name, line, attributes);
        if (dn == null) {
            log.debug("Unparsable line: "+line);
            return null;
        }

        return new SearchResult(dn, attributes);
    }

    public DN parse(
            String base,
            String type,
            String name,
            String line,
            Attributes attributes
    ) throws Exception {

        log.debug("Parsing "+name+": "+line+" ("+type +")");

        if ("posixAccount".equals(type)) {
            return parsePosixAccount(name, line, attributes);

        } else if ("shadowAccount".equals(type)) {
            return parseShadowAccount(name, line, attributes);

        } else if ("ipHost".equals(type)) {
            return parseIPHost(name, line, attributes);

        } else if ("posixGroup".equals(type)) {
            return parsePosixGroup(name, line, attributes);

        } else if ("ipService".equals(type)) {
            return parseIPService(name, line, attributes);

        } else if ("oncRpc".equals(type)) {
            return parseONCRpc(name, line, attributes);

        } else if ("nisNetId".equals(type)) {
            return parseNISNetId(name, line, attributes);

        } else if ("ipProtocol".equals(type)) {
            return parseIPProtocol(name, line, attributes);

        } else if ("nisMailAlias".equals(type)) {
            return parseNISMailAlias(name, line, attributes);

        } else if ("nisNetgroup".equals(type)) {
            return parseNISNetgroup(name, line, attributes);

        } else if ("ieee802Device".equals(type)) {
            return parseIEEE802Device(name, line, attributes);

        } else if ("bootableDevice".equals(type)) {
            return parseBootableDevice(name, line, attributes);

        } else if ("ipNetwork".equals(type)) {
            return parseIPNetwork(name, line, attributes);

        } else if ("automountMap".equals(type)) {
            return parseAutomountMap(name, line, attributes);

        } else if ("automount".equals(type)) {
            return parseAutomount(base, name, line, attributes);

        } else if ("nisMap".equals(type)) {
            return parseNISMap(name, attributes);
        }

        return parseNISObject(type, name, line, attributes);
    }

    public DN parsePosixAccount(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        String tokens[] = line.split(":");

        if (name == null) {
            name = tokens[0];
        }

        rb.set("uid", name);
        attributes.setValue("primaryKey.uid", name);
        attributes.setValue("uid", name);

        String userPassword = tokens[1];
        if (!"".equals(userPassword)
                && !"!!".equals(userPassword)
                && !"*".equals(userPassword)
                && !"x".equals(userPassword)
                && !userPassword.startsWith("##")) {
            attributes.setValue("userPassword", userPassword);
        }

        attributes.setValue("uidNumber", tokens[2]);
        attributes.setValue("gidNumber", tokens[3]);

        String gecos = tokens[4];
        if (!"".equals(gecos)) attributes.setValue("gecos", gecos);

        if (tokens.length>5) attributes.setValue("homeDirectory", tokens[5]);
        if (tokens.length>6) attributes.setValue("loginShell", tokens[6]);

        return new DN(rb.toRdn());
    }

    public DN parseShadowAccount(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        String tokens[] = line.split(":");

        if (name == null) {
            name = tokens[0];
        }

        rb.set("uid", name);
        attributes.setValue("primaryKey.uid", name);
        attributes.setValue("uid", name);

        DN dn = new DN(rb.toRdn());

        if (tokens.length <= 1) return dn;

        String userPassword = tokens[1];
        if (!"".equals(userPassword)
                && !"!!".equals(userPassword)
                && !"*".equals(userPassword)
                && !"x".equals(userPassword)
                && !userPassword.startsWith("##")) {
            attributes.setValue("userPassword", userPassword);
        }

        if (tokens.length <= 2) return dn;

        String s = tokens[2];
        if (!"".equals(s)) attributes.setValue("shadowLastChange", s);

        if (tokens.length <= 3) return dn;

        s = tokens[3];
        if (!"".equals(s)) attributes.setValue("shadowMin", s);

        if (tokens.length <= 4) return dn;

        s = tokens[4];
        if (!"".equals(s)) attributes.setValue("shadowMax", s);

        if (tokens.length <= 5) return dn;

        s = tokens[5];
        if (!"".equals(s)) attributes.setValue("shadowWarning", s);

        s = tokens[6];
        if (tokens.length <= 6) return dn;

        if (!"".equals(s)) attributes.setValue("shadowInactive", s);

        if (tokens.length <= 7) return dn;

        s = tokens[7];
        if (!"".equals(s)) attributes.setValue("shadowExpire", s);

        if (tokens.length <= 8) return dn;

        s = tokens[8];
        if (!"".equals(s)) attributes.setValue("shadowFlag", s);

        return dn;
    }

    public DN parseIPHost(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        StringTokenizer st = new StringTokenizer(line, "\t ");

        attributes.setValue("ipHostNumber", st.nextToken());

        name = st.nextToken();

        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        while (st.hasMoreTokens()) {
            name = st.nextToken();
            if (name.startsWith("#")) break;
            attributes.addValue("cn", name);
        }

        return new DN(rb.toRdn());
    }

    public DN parsePosixGroup(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        String []tokens = line.split(":");

        if (name == null) {
            name = tokens[0];
        }

        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        String userPassword = tokens[1];
        if (!"".equals(userPassword) && !"!!".equals(userPassword) && !"*".equals(userPassword) && !"x".equals(userPassword)) {
            attributes.setValue("userPassword", userPassword);
            //attributes.set("userPassword", BinaryUtil.decode(BinaryUtil.BASE64, userPassword));
        }

        attributes.setValue("gidNumber", tokens[2]);

        if (tokens.length > 3) {
            String members = tokens[3];
            tokens = members.split(",");

            for (String token : tokens) {
                attributes.addValue("memberUid", token);
            }
        }

        return new DN(rb.toRdn());
    }

    public DN parseIPService(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        StringTokenizer st = new StringTokenizer(line, "\t ");

        attributes.setValue("name", name);

        String cn = st.nextToken();
        attributes.setValue("cn", cn);

        String []tokens = st.nextToken().split("/");
        attributes.setValue("ipServicePort", tokens[0]);
        attributes.setValue("ipServiceProtocol", tokens[1]);

        attributes.setValue("primaryKey.ipServicePort", tokens[0]);
        attributes.setValue("primaryKey.ipServiceProtocol", tokens[1]);

        rb.set("ipServicePort", tokens[0]);
        rb.set("ipServiceProtocol", tokens[1]);

        while (st.hasMoreTokens()) {
            cn = st.nextToken();
            if (cn.startsWith("#")) break;
            attributes.addValue("cn", cn);
        }

        return new DN(rb.toRdn());
    }

    public DN parseONCRpc(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        StringTokenizer st = new StringTokenizer(line, "\t ");

        name = st.nextToken();

        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        attributes.setValue("oncRpcNumber", st.nextToken());

        while (st.hasMoreTokens()) {
            name = st.nextToken();
            if (name.startsWith("#")) break;
            attributes.addValue("cn", name);
        }

        return new DN(rb.toRdn());
    }

    public DN parseNISNetId(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        if (name == null) {
            int i = line.indexOf(' ');
            name = line.substring(0, i);
            line = line.substring(i+1).trim();
        }

        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        DN dn = new DN(rb.toRdn());

        int i = name.indexOf(".");
        int j = name.indexOf("@", i+1);

        if (i > 0 && j > 0) {
            String s = name.substring(i+1, j);
            try {
                // unix.uid@domain uid:gid,gid,gid,...
                Long.parseLong(s);

            } catch (Exception e) {
                // unix.hostname@domain 0:hostname
                attributes.addValue("nisNetIdHost", s);
                return dn;
            }

        } else { // nobody uid:gid,gid
            // ignore
        }

        int k = line.indexOf(':');

        String uid = line.substring(0, k);
        attributes.setValue("nisNetIdUser", uid);

        String remainder = line.substring(k+1).trim();

        StringTokenizer st = new StringTokenizer(remainder, ",\t ");
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (token.startsWith("#")) break;
            attributes.addValue("nisNetIdGroup", token);
        }

        return dn;
    }

    public DN parseIPProtocol(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        StringTokenizer st = new StringTokenizer(line, "\t ");

        name = st.nextToken();

        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        attributes.setValue("ipProtocolNumber", st.nextToken());

        while (st.hasMoreTokens()) {
            name = st.nextToken();
            if (name.startsWith("#")) break;
            attributes.addValue("cn", name);
        }

        return new DN(rb.toRdn());
    }

    public DN parseNISMailAlias(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        if (name == null) {
            int i = line.indexOf(':');
            name = line.substring(0, i);
            line = line.substring(i+1).trim();
        }
        
        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        StringBuilder sb = new StringBuilder();

        char[] chars = line.toCharArray();
        boolean done = false;

        for (int i=0; i<chars.length && !done; i++) {
            char c = chars[i];

            switch (c) {
                case ',':
                case '#':
                    String value = sb.toString().trim();
                    if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length()-1);
                    }
                    if (value.length() > 0) attributes.addValue("rfc822mailMember", value);

                    sb.delete(0, sb.length());

                    if (c == '#') {
                        String comment = line.substring(i+1).trim();
                        if (comment.length() > 0) attributes.addValue("description", comment);
                        done = true;
                    }

                    break;

                default:
                    sb.append(c);
                    break;
            }
        }

        if (sb.length() > 0) {
            String value = sb.toString().trim();
            if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length()-1);
            }
            if (value.length() > 0) attributes.addValue("rfc822mailMember", value);
        }

        return new DN(rb.toRdn());
    }

    public DN parseNISNetgroup(String name, String line, Attributes attributes) throws Exception {

        //log.warn("Parsing ["+name+"] ["+line+"]");

        List<String> tokens = new ArrayList<String>();

        StringBuilder sb = new StringBuilder();
        int length = line.length();
        char c;

        for (int i = 0; i < length; i++) {

            c = line.charAt(i);

            while (i < length-1 && Character.isWhitespace(c)) { // skip whitespaces
                i++;
                c = line.charAt(i);
            }

            if (i >= length) break;

            sb.append(c); // append token's first char

            if (c == '(') { // if nisNetgroupTriple

                while (i < length-1) { // find closing parentheses
                    i++;
                    c = line.charAt(i);
                    sb.append(c);
                    if (c == ')') break;
                }

            } else { // if memberNisNetgroup

                while (i < length-1) { // find next whitespace
                    i++;
                    c = line.charAt(i);
                    if (Character.isWhitespace(c)) break;
                    sb.append(c);
                }

            }

            tokens.add(sb.toString());
            sb.setLength(0);
        }

        RDNBuilder rb = new RDNBuilder();
        rb.set("cn", name);

        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        while (!tokens.isEmpty()) {
            String token = tokens.remove(0);
            //log.warn(" - ["+token+"]");

            if (token.startsWith("#")) break;
            if (token.startsWith("(") && token.endsWith(")")) {
                //token = token.substring(1, token.length()-1);
                attributes.addValue("nisNetgroupTriple", token);
            } else {
                attributes.addValue("memberNisNetgroup", token);
            }
        }

        return new DN(rb.toRdn());
    }

    public DN parseIEEE802Device(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        StringTokenizer st = new StringTokenizer(line, "\t ");

        attributes.setValue("macAddress", st.nextToken());

        if (name == null) {
            name = st.nextToken();
        }

        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        while (st.hasMoreTokens()) {
            name = st.nextToken();
            if (name.startsWith("#")) break;
            attributes.addValue("cn", name);
        }

        return new DN(rb.toRdn());
    }

    public DN parseBootableDevice(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        StringTokenizer st = new StringTokenizer(line, "\t ");

        if (name == null) {
            name = st.nextToken();
        }

        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (token.startsWith("#")) break;
            attributes.addValue("bootParameter", token);
        }

        return new DN(rb.toRdn());
    }

    public DN parseIPNetwork(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        StringTokenizer st = new StringTokenizer(line, "\t ");

        name = st.nextToken();

        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        attributes.setValue("ipNetworkNumber", st.nextToken());

        while (st.hasMoreTokens()) {
            name = st.nextToken();
            if (name.startsWith("#")) break;
            attributes.addValue("cn", name);
        }

        return new DN(rb.toRdn());
    }

    public DN parseAutomountMap(String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        if (name == null) {
            int i = line.indexOf(' ');
            name = line.substring(0, i);
            line = line.substring(i+1).trim();
        }

        rb.set("automountKey", name);

        attributes.setValue("primaryKey.automountKey", name);

        attributes.setValue("automountKey", name);
        attributes.setValue("automountInformation", line);

        return new DN(rb.toRdn());
    }

    public DN parseAutomount(String base, String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        if (name == null) {
            int i = line.indexOf(' ');
            name = line.substring(0, i);
            line = line.substring(i+1).trim();
        }

        rb.set("automountMapName", base);
        rb.set("automountKey", name);

        attributes.setValue("primaryKey.automountMapName", base);
        attributes.setValue("primaryKey.automountKey", name);
        
        attributes.setValue("automountMapName", base);
        attributes.setValue("automountKey", name);
        attributes.setValue("automountInformation", line);

        return new DN(rb.toRdn());
    }

    public DN parseNISMap(String name, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        rb.set("nisMapName", name);
        attributes.setValue("primaryKey.nisMapName", name);
        attributes.setValue("nisMapName", name);

        return new DN(rb.toRdn());
    }

    public DN parseNISObject(String mapName, String name, String line, Attributes attributes) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        if (name == null) {
            int i = line.indexOf(' ');
            name = line.substring(0, i);
            line = line.substring(i+1).trim();
        }
        
        rb.set("cn", name);
        attributes.setValue("primaryKey.cn", name);
        attributes.setValue("cn", name);

        attributes.setValue("nisMapEntry", line);
        attributes.setValue("nisMapName", mapName);

        return new DN(rb.toRdn());
    }

}
