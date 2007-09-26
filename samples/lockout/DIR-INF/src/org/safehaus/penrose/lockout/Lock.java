package org.safehaus.penrose.lockout;

import java.util.Date;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.text.DateFormat;

/**
 * @author Endi Sukma Dewata
 */
public class Lock implements Serializable {

    public final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected String account;
    protected int counter;
    protected Date timestamp;

    public Lock(String account, int counter, Date timestamp) {
        this.account = account;
        this.counter = counter;
        this.timestamp = timestamp;
    }
    
    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}
