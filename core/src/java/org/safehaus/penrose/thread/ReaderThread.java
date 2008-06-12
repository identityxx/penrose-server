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
package org.safehaus.penrose.thread;

import java.io.Reader;
import java.io.Writer;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Endi Sukma Dewata
 */
public class ReaderThread extends Thread {

    private Reader reader;
    private Writer writer;

    private InputStream is;
    private OutputStream os;

    public ReaderThread(Reader in, Writer out) throws Exception {
        this.reader = in;
        this.writer = out;
    }

    public ReaderThread(InputStream in, OutputStream out) throws Exception {
        this.is = in;
        this.os = out;
    }

   public void run() {
        try {
            if (reader != null) {
                runReaderWriter();
            } else {
                runInputOutputStream();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runReaderWriter() throws Exception {
        int c;
        while ((c = reader.read()) != -1) {
            writer.write(c);
            writer.flush();
        }
        reader.close();
    }

    public void runInputOutputStream() throws Exception {
        int c;
        while ((c = is.read()) != -1) {
            os.write(c);
            os.flush();
        }
        os.close();
    }
}

