/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.etrust;

import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;

import java.io.*;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.Date;

/**
 * @author Endi S. Dewata
 */
public class ETrustDirectoryRunnable implements Runnable, FilenameFilter {

    Logger log = Logger.getLogger(getClass());

    ETrustDirectoryModule module;
    String serverName;

    boolean running = true;

    public ETrustDirectoryRunnable(ETrustDirectoryModule service, String serverName) {
        this.module = service;
        this.serverName = serverName;
    }

    public boolean accept(File file, String filename) {
        return filename.startsWith(serverName+"_update_") && filename.endsWith(".log");
    }

    public void run() {
        try {
            runImpl();
        } catch (Exception e) {
            Penrose.errorLog.error(e.getMessage(), e);
        }
    }

    public void runImpl() throws Exception {

        String logsDir = module.home+File.separator+"dxserver"+File.separator+"logs";
        File dir = new File(logsDir);

        log.debug("Getting the last log file in "+logsDir);

        String filename = null;

        while (running && filename == null) {
            String filenames[] = dir.list(this);

            if (filenames.length == 0) {
                Thread.sleep(10 * 1000);

            } else {
                TreeSet set = new TreeSet();
                set.addAll(Arrays.asList(filenames));
                filename = (String)set.last();
            }
        }

        log.debug("Log file: "+filename);

        File file = new File(dir, filename);
        long length = file.length();

        while (running) {

            Thread.sleep(module.interval * 1000);

            log.debug("Checking "+filename);

            long length2 = file.length();
            if (length != length2) {
                process(file, length);
                length = length2;
            }

            Date date = new Date();
            String newFilename = serverName+"_update_"+module.dateFormat1.format(date)+".log";
            File newFile = new File(dir, newFilename);

            if (!filename.equals(newFilename) && newFile.exists()) {

                log.debug("New log file: "+filename);

                length = 0;
                filename = newFilename;
                file = newFile;

                process(file, length);
            }
        }
    }

    public void process(File file, long start) throws Exception {
        FileInputStream is = new FileInputStream(file);
        is.skip(start);

        BufferedReader in = new BufferedReader(new InputStreamReader(is));

        String line;
        while ((line = in.readLine()) != null) {
            //log.debug(" - "+line);
            try {
                process(line);
            } catch (Exception e) {
                log.debug(e.getMessage(), e);
            }
        }

        in.close();
    }

    public void process(String line) throws Exception {
        int p = line.indexOf(' ');
        String timestamp = line.substring(0, p);
        Date date = module.dateFormat2.parse(timestamp);
        line = line.substring(p+1);

        String user;
        if (line.charAt(0) == '\"') {
            p = line.indexOf('\"', 1);
            user = line.substring(1, p);
            line = line.substring(p+2);
        } else {
            p = line.indexOf(' ');
            user = line.substring(0, p);
            line = line.substring(p+1);

            if ("(none)".equals(user)) user = null;
        }

        p = line.indexOf(' ');
        String action = line.substring(0, p);
        String data = line.substring(p+1);

        module.process(date, user, action, data);
    }

    public void stop() {
        running = false;
    }
}
