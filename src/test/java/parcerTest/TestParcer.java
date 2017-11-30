package parcertest;


import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import parcer.Parcer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestParcer {

    @Test
    public void readingTest () throws ParseException, IOException{
        String testStr1 = "78.123.67.3, https://en.wikipedia.org/wiki/Main_Page, 26/Nov/2017:00:24:43 +0200, 200";
        String testStr2 = "78.123.67.3, https://en.wikipedia.org/wiki/World_War_I, 26/Nov/2017:00:24:46 +0200, 1600";
        try {
            FileWriter writer = new FileWriter("testLog.csv");
            writer.write("IP, URL, timeStamp, timeSpent" + "\n");
            writer.write(testStr1 + "\n");
            writer.write(testStr2 + "\n");
            writer.close();
        } catch (IOException ex){
            System.out.println("createDirectory failed:");
        }
        List<String> res = Parcer.read("files/testLog.csv");
        assertEquals(res, new ArrayList<>(Arrays.asList(testStr1, testStr2)));
    }

    @Test
    public void parcingTest() throws IOException, ParseException{
        String testStr1 = "78.123.67.3, https://en.wikipedia.org/wiki/Main_Page, 26/Nov/2017:00:24:43 +0200, 200";
        String testStr2 = "78.123.67.3, https://en.wikipedia.org/wiki/World_War_I, 26/Nov/2017:00:24:46 +0200, 1600";
        String testJson1 = "{\"ip\":\"78.123.67.3\",\"url\":\"https://en.wikipedia.org/wiki/Main_Page\"," +
                "\"timeStamp\":\"Nov 26, 2017 12:24:43 AM\",\"timeSpent\":200}";
        String testJson2 = "{\"ip\":\"78.123.67.3\",\"url\":\"https://en.wikipedia.org/wiki/World_War_I\"," +
                "\"timeStamp\":\"Nov 26, 2017 12:24:46 AM\",\"timeSpent\":1600}";

        List<String> result = Parcer.convertToJson(Arrays.asList(testStr1, testStr2));
        List<String> rigthResult = Arrays.asList(testJson1, testJson2);

        assertEquals(result, rigthResult);
    }
}
