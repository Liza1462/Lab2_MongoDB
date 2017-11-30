package queriestest;

import com.mongodb.client.MongoCollection;

import java.util.*;

import dbqueries.LogsDB;
import org.junit.Test;

import java.io.IOException;

import java.util.List;

import static org.junit.Assert.*;



public class TestLogDB {

    public static LogsDB testdb = new LogsDB("logDB");
    public static  MongoCollection<org.bson.Document> logs = testdb.getCollection("testLogs1");
    public static MongoCollection<org.bson.Document> logs2 = testdb.getCollection("testLogs2");

    public static void insert() throws IOException{
        logs.drop();
        String s1 = "{\"ip\":\"78.123.67.3\",\"url\":\"https://en.wikipedia.org/wiki/Main_Page\"," +
                "\"timeStamp\":\"Nov 26, 2017 12:24:43 AM\",\"timeSpent\":200}\n";
        String s2 = "{\"ip\":\"157.55.39.105\",\"url\":\"https://en.wikipedia.org/wiki/Nikita_Khrushchev\"" +
                ",\"timeStamp\":\"Nov 26, 2017 1:20:39 AM\",\"timeSpent\":600}\n";
        String s3 ="{\"ip\":\"40.77.167.48\",\"url\":\"https://en.wikipedia.org/wiki/Kim_Kardashian\"" +
                ",\"timeStamp\":\"Nov 27, 2017 1:00:25 PM\",\"timeSpent\":678}\n";
        String s4 = "{\"ip\":\"40.77.167.48\",\"url\":\"https://en.wikipedia.org/wiki/Main_Page\",\"" +
                "timeStamp\":\"Nov 26, 2017 12:24:45 AM\",\"timeSpent\":378}\n";
        String s5 = "{\"ip\":\"157.55.39.105\",\"url\":\"https://en.wikipedia.org/wiki/Soviet_Union\",\"" +
                "timeStamp\":\"Nov 26, 2017 1:05:29 AM\",\"timeSpent\":780}";
        String s6 = "{\"ip\":\"40.77.167.48\",\"url\":\"https://en.wikipedia.org/wiki/Soviet_Union\",\"" +
                "timeStamp\":\"Nov 26, 2017 12:30:45 AM\",\"timeSpent\":1000}\n";

        logs.insertOne(org.bson.Document.parse(s1));
        logs.insertOne(org.bson.Document.parse(s2));
        logs.insertOne(org.bson.Document.parse(s3));
        logs.insertOne(org.bson.Document.parse(s4));
        logs.insertOne(org.bson.Document.parse(s5));
        logs.insertOne(org.bson.Document.parse(s6));
    }

    @Test
    public void testGetIP() throws IOException{
        insert();
        List <String> result = LogsDB.getIPs("https://en.wikipedia.org/wiki/Main_Page", logs);
        assertEquals( Arrays.asList("40.77.167.48", "78.123.67.3"), result);
    }

    @Test
    public void testGetURL(){
        List <String> result = LogsDB.getURLs("40.77.167.48", logs);
        assertEquals(Arrays.asList("https://en.wikipedia.org/wiki/Kim_Kardashian",
                "https://en.wikipedia.org/wiki/Main_Page",
                "https://en.wikipedia.org/wiki/Soviet_Union"), result);
    }

//    @Test
//    public void testGetURLsByTime() throws ParseException{
//        List <String> result = dbqueries.LogsDB.getURLs("26/Nov/2017:01:30:45","26/Nov/2017:01:30:45", logs);
//        assertEquals( Arrays.asList("40.77.167.48", "78.123.67.3"), result);
//    }

}
