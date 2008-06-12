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
package org.safehaus.penrose.nis.module;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.thread.ReaderThread;
import org.safehaus.penrose.util.TextUtil;

import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Endi Sukma Dewata
 */
public class NISModule extends Module {

    public final static String SCP           = "scp";
    public final static String DEFAULT_SCP   = "/usr/bin/scp";

    public final static String SSH           = "ssh";
    public final static String DEFAULT_SSH   = "/usr/bin/ssh";

    public final static String PERL          = "perl";
    public final static String DEFAULT_PERL  = "/usr/bin/perl";

    public final static String RM            = "rm";
    public final static String DEFAULT_RM    = "/bin/rm";

    public final static String MKDIR         = "mkdir";
    public final static String DEFAULT_MKDIR = "/bin/mkdir";

    String scp;
    String ssh;
    String perl;
    String rm;
    String mkdir;

    public void init() throws Exception {
        scp = moduleConfig.getParameter(SCP);
        if (scp == null) scp = DEFAULT_SCP;

        ssh = moduleConfig.getParameter(SSH);
        if (ssh == null) ssh = DEFAULT_SSH;

        perl = moduleConfig.getParameter(PERL);
        if (perl == null) perl = DEFAULT_PERL;

        rm = moduleConfig.getParameter(RM);
        if (rm == null) rm = DEFAULT_RM;

        mkdir = moduleConfig.getParameter(MKDIR);
        if (mkdir == null) mkdir = DEFAULT_MKDIR;
    }

    public void scan(String hostname, String[] paths) throws Exception {

        if (debug) log.debug("Scanning "+hostname);

        PrintWriter out = new PrintWriter(System.out, true);
        PrintWriter err = new PrintWriter(System.err, true);

        PenroseContext penroseContext = partition.getPartitionContext().getPenroseContext();
        String localPath = penroseContext.getHome()+"/samples/nis_tool";
        String remotePath = "/tmp/penrose";
        String script = "scan.pl";

        ssh(out, err, hostname, new String[] { mkdir, "-p", remotePath });
        scp(localPath+"/bin", hostname+":"+remotePath);

        for (String path : paths) {
            ssh(out, err, hostname, new String[] {
                perl,
                remotePath+"/bin/"+script,
                path
            });
        }

        ssh(out, err, hostname, new String[] { rm, "-rf", remotePath });
    }

    public int scp(String source, String target) throws Exception {

        PrintWriter out = new PrintWriter(System.out, true);
        PrintWriter err = new PrintWriter(System.err, true);

        String[] command = new String[] {
                scp,
                "-r",
                source,
                target
        };

        return execute(out, err, command);
    }

    public int ssh(PrintWriter out, PrintWriter err, String hostname, String[] args) throws Exception {

        Collection<String> command = new ArrayList<String>();
        command.add(ssh);
        command.add(hostname);
        command.addAll(Arrays.asList(args));

        return execute(out, err, command.toArray(new String[command.size()]));
    }

    public int execute(PrintWriter out, PrintWriter err, String[] command) throws Exception {

        if (debug) {
            StringBuilder sb = new StringBuilder();
            for (String s : command) {
                if (sb.length() > 0) sb.append(" ");
                sb.append(s);
            }

            log.debug(TextUtil.displaySeparator(80));
            Collection<String> lines = TextUtil.split(sb.toString(), 80);
            for (String line : lines) {
                log.debug(TextUtil.displayLine(line, 80));
            }
            log.debug(TextUtil.displaySeparator(80));
        }

        Runtime rt = Runtime.getRuntime();
        Process p = rt.exec(command);

        new ReaderThread(new InputStreamReader(p.getInputStream()), out).start();
        new ReaderThread(new InputStreamReader(p.getErrorStream()), err).start();

        int rc = p.waitFor();

        p.getOutputStream().close();
        
        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("RC: "+rc, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        return rc;
    }
}
