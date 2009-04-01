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
package org.safehaus.penrose.util;

import java.io.*;

import org.apache.log4j.Logger;

public class FileUtil {
	
	public static Logger log = Logger.getLogger(FileUtil.class);

    public static void copy(String filename1, String filename2) throws Exception {
        File file1 = new File(filename1);
        File file2 = new File(filename2);
        copy(file1, file2);
    }

    public static void copy(File file1, File file2) throws Exception {
        if (file1.getAbsoluteFile().equals(file2.getAbsoluteFile())) return;
        
        if (file1.isDirectory()) {
            copyFolder(file1, file2);
            return;
        }

        File parent = file2.getParentFile();
        if (parent != null) parent.mkdirs();

        FileInputStream in = new FileInputStream(file1);
        FileOutputStream out = new FileOutputStream(file2);

        byte[] buffer = new byte[4096];

        int c;
        while ((c = in.read(buffer)) != -1) out.write(buffer, 0, c);

        out.close();
        in.close();
    }

    public static void copyFolder(String dirname1, String dirname2) throws Exception {
        File dir1 = new File(dirname1);
        File dir2 = new File(dirname2);
        copyFolder(dir1, dir2);
    }

    public static void copyFolder(File dir1, File dir2) throws Exception {
        dir2.mkdirs();

        File files[] = dir1.listFiles();
        if (files != null) {
            for (File file : files) {
                File target = new File(dir2, file.getName());

                if (file.isDirectory()) {
                    copyFolder(file, target);
                } else {
                    copy(file, target);
                }
            }
        }
    }

    public static void delete(String dir) {
        delete(new File(dir));
    }

    public static void delete(File dir) {
        File files[] = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                delete(file);
            }
        }
        dir.delete();
    }

	public static String getContent(File file) throws IOException {
        StringBuilder sb = new StringBuilder();

		FileReader r = new FileReader(file);
		BufferedReader br = new BufferedReader(r);

        String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}

        r.close();

		return sb.toString();
	}
}