package dbqueries;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.mongodb.client.MongoDatabase;


import static com.mongodb.client.model.Filters.*;

public class LogsDB {

    private static MongoDatabase database;
    private static  DB db;
//    public static MongoCollection<Document> logs;
//    public static DBCollection collection;

    public LogsDB(String databaseName){
        connect(databaseName);
    }

//    public static void main(String[] args) throws IOException{
//        connect("logDB");
//        MongoCollection<Document> coll = database.getCollection("testLogs1");
//        DBCollection collMR = db.getCollection("testLogs1");
//        insertFromJson("files/result.txt", coll);
//        getIPs("https://en.wikipedia.org/wiki/Kim_Kardashian");
//        getURLs("157.55.39.105");
//        getIPMR();
//        getURLMR();
//        getVisitsCount();
//        getTime(collMR);
//    }

    public static MongoCollection<Document> getCollection( String collectionName){
         return database.getCollection(collectionName);
    }

    public static DBCollection getCollectionMR( String collectionName){
        return db.getCollection(collectionName);
    }

    public static void connect(String databaseName){
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        database = mongoClient.getDatabase(databaseName);

        MongoClient mongo = new MongoClient("localhost", 27017);
        db = mongo.getDB(databaseName);

        System.out.println("Connected to DB");
    }

    //  Разработайте запросы для загрузки полученных данных в формате JSON в СУБД MongoDB.
    public static void insertFromJson(String filename, MongoCollection<Document> logs) throws IOException {
        try (BufferedReader in = new BufferedReader(new FileReader(new File(filename)))) {
            in.lines().forEach(line -> {
                Document doc = Document.parse(line);
                logs.insertOne(doc);
            });
        } catch (FileNotFoundException ex){
            System.out.println("File not found");
        }
    }

    public static List<String> getAllCollectionElements(MongoCollection<Document> logs){
        List <String> res = new ArrayList<>();
        for (Document log : logs.find()){
            res.add(log.values().toString());
        }
        return res;
    }

        // Выдать упорядоченный список IP-адресов пользователей, посетивших ресурс с заданным URL.
    public static List<String> getIPs(String URL, MongoCollection<Document> logs){
        List <String> ips = new ArrayList<>();
        for (Document log : logs.find(eq("url", URL)).sort(Sorts.ascending("ip"))){
            ips.add(log.getString("ip"));
        }
        return ips;
    }

        // Выдать упорядоченный список URL ресурсов, посещенных пользователем сзаданным IP-адресом.
    public static List<String> getURLs(String ip, MongoCollection<Document> logs){
        List <String> urls = new ArrayList<>();
        for (Document log : logs.find(eq("ip", ip)).sort(Sorts.ascending("url"))){
            urls.add(log.getString("url"));
        }
        return urls;
    }

//        // Выдать упорядоченный список URL ресурсов, посещенных в заданный временной период.
//    public static List<String> getURLs(String startDate, String endDate, MongoCollection<Document> logs) throws ParseException{
//        List <String> urls = new ArrayList<>();
//        DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
//        Date sd = format.parse(startDate);
//        Date ed = format.parse(endDate);
//        System.out.println(sd);
//        System.out.println(ed);
//        for (Document log : logs.find(and(gte("timeStamp", sd), lte("timeStamp", ed))).sort(Sorts.ascending("url"))){
//            urls.add(log.getString("url"));
//        }
//        return urls;
//    }
//Выдать список URL ресурсов с указанием суммарной длительности
//посещения каждого ресурса, упорядоченный по убыванию.
    public static List<String> getTime(DBCollection collection){
        String map = "function () { emit(this.url, this.timeSpent);}";
        String reduce = "function (key, values) { "+
                " totalTime = 0; "+
                " for (var i in values) { totalTime += values[i]} "+
                " return  totalTime }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);
        MapReduceOutput out = collection.mapReduce(cmd);
        List<String> result = new ArrayList<>();
        for (DBObject o : out.results()) {
            result.add(o.toMap().values().toArray()[0].toString() +" : "
                    + o.toMap().values().toArray()[1].toString());
        }
        return result;
    }

//Выдать список URL ресурсов с указанием суммарного количества
//посещений каждого ресурса, упорядоченный по убыванию.
    public static List<String> getVisitsCount(DBCollection collection){
        String map = "function () { emit(this.url, 1);}";
        String reduce = "function (key, values) { "+
                " visitsCount = 0; "+
                " for (var i in values) { visitsCount += values[i]} "+
                " return visitsCount }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);
        MapReduceOutput out = collection.mapReduce(cmd);
        List<String> result = new ArrayList<>();
        for (DBObject o : out.results()) {
            result.add(o.toMap().values().toArray()[0].toString() + " : " + o.toMap().values().toArray()[1].toString());
        }
        return result;
    }

//    Выдать список IP-адресов c указанием суммарного количества и суммарной
//    длительности посещений ресурсов, упорядоченный по адресу, убыванию
//    количества и убыванию длительности.
    public static List<String> getIPMR(DBCollection collection){
        String map = "function () { emit(this.ip, {time:this.timeSpent, count: 1} );}";
        String reduce = "function (key, values) { "+
                " count = 0; " +
                " time = 0;"+
                " for (var i in values) { " +
                "count += values[i].count; " +
                "time += values[i].time}"+
                " return {time, count} }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);
        MapReduceOutput out = collection.mapReduce(cmd);
        List<String> result = new ArrayList<>();
        for (DBObject o : out.results()) {
            result.add(o.toMap().values().toArray()[0].toString() + "  totalTime: "+
                    o.toMap().values().toArray()[1].toString().split(" ")[3] + ", totalCount: " +
                    o.toMap().values().toArray()[1].toString().split(" ")[7].replaceAll("}", ""));
        }
        return result;
    }

//    Выдать список URL ресурсов с указанием количества посещений каждого
//    ресурса в день за заданный период, упорядоченный по URL ресурса и
//    убыванию количества посещений.
    public static List<String> getURLMR(DBCollection collection){
        String map = "function () { " +
                "let date = new Date(this.timeStamp.split(' ')).toDateString();" +
                "emit({url: this.url, date: date}, {count: 1} );}";
        String reduce = "function (key, values) { "+
                " visitsCount = 0; " +
                " for (var i in values) { " +
                "visitsCount += values[i].count} " +
                " return {'count' : visitsCount} }";
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                null, MapReduceCommand.OutputType.INLINE, null);
        MapReduceOutput out = collection.mapReduce(cmd);
        List<String> result = new ArrayList<>();
        for (DBObject o : out.results()) {
            result.add(o.toMap().values().toArray()[0].toString().split("\"")[3] + " date: "+
                    o.toMap().values().toArray()[0].toString().split("\"")[7]+ " count: "+
                    o.toMap().values().toArray()[1].toString().split(": ")[1].replaceAll("}", ""));
        }
        return result;
    }
}
