package parcer;
import com.google.gson.Gson;

import java.io.*;
import java.util.Date;

public class Log {
    private String ip;
    private String url;
    private Date timeStamp;
    private int timeSpent;

    public Log(String ip, String url, Date timeStamp, int timeSpent) {
        this.ip = ip;
        this.url = url;
        this.timeSpent = timeSpent;
        this.timeStamp = timeStamp;
    }

    public String convertToJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return "log (url=" + this.url + ", ip=" + this.ip + ", timeStamp=" + this.timeStamp + ", timeSpent=" + this.timeSpent+ ")";
    }
}
