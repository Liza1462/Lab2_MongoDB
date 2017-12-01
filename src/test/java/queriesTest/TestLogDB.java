package queriestest;

import com.mongodb.DBCollection;
import com.mongodb.client.MongoCollection;

import java.lang.reflect.Array;
import java.util.*;

import com.mongodb.client.model.Sorts;
import dbqueries.LogsDB;
import org.bson.Document;
import org.junit.Test;

import java.io.IOException;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.*;



public class TestLogDB {

    public static LogsDB testdb = new LogsDB("logDB");
    public static  MongoCollection<org.bson.Document> logs = testdb.getCollection("testLogs1");
    public static DBCollection logs2 = testdb.getCollectionMR("testLogs1");

    @Test
    public void testInsert() throws IOException{
        logs.drop();

       LogsDB.insertFromJson("files/testRes", logs);

        List <String> res = new ArrayList<>();
        for (Document log : logs.find()){
            res.add(log.toJson());
        }

        assertEquals(res.size(), 6);
    }

    @Test
    public void testGetIP() throws IOException{
//        insert();
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

    @Test
    public void testGetTime(){
        List <String> rightRes = new ArrayList<>();
        rightRes.add("https://en.wikipedia.org/wiki/Kim_Kardashian : 678.0");
        rightRes.add("https://en.wikipedia.org/wiki/Main_Page : 578.0");
        rightRes.add("https://en.wikipedia.org/wiki/Nikita_Khrushchev : 600.0");
        rightRes.add("https://en.wikipedia.org/wiki/Soviet_Union : 1780.0");
        assertEquals(rightRes, LogsDB.getTime(logs2));
    }

    @Test
    public void testGetVisitsCount(){
        List <String> rightRes = new ArrayList<>();
        rightRes.add("https://en.wikipedia.org/wiki/Kim_Kardashian : 1.0");
        rightRes.add("https://en.wikipedia.org/wiki/Main_Page : 2.0");
        rightRes.add("https://en.wikipedia.org/wiki/Nikita_Khrushchev : 1.0");
        rightRes.add("https://en.wikipedia.org/wiki/Soviet_Union : 2.0");
        assertEquals(rightRes, LogsDB.getVisitsCount(logs2));
    }

    @Test
    public void testGetIPMR(){
        List <String> rightRes = new ArrayList<>();
        rightRes.add("157.55.39.105  totalTime: 1380.0, totalCount: 2.0");
        rightRes.add("40.77.167.48  totalTime: 2056.0, totalCount: 3.0");
        rightRes.add("78.123.67.3  totalTime: 200.0, totalCount: 1.0");
        assertEquals(rightRes, LogsDB.getIPMR(logs2));
    }

    @Test
    public void testGetURLMR(){
        List <String> rightRes = new ArrayList<>();
        rightRes.add("https://en.wikipedia.org/wiki/Kim_Kardashian date: Mon Nov 27 2017 count: 1.0");
        rightRes.add("https://en.wikipedia.org/wiki/Main_Page date: Sun Nov 26 2017 count: 2.0");
        rightRes.add("https://en.wikipedia.org/wiki/Nikita_Khrushchev date: Sun Nov 26 2017 count: 1.0");
        rightRes.add("https://en.wikipedia.org/wiki/Soviet_Union date: Sun Nov 26 2017 count: 2.0");
        assertEquals(rightRes, LogsDB.getURLMR(logs2));
    }





}
