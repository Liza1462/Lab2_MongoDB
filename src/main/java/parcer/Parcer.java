package parcer;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


public class Parcer {

    public static List<String> read(String filename) throws IOException{
        try (BufferedReader in = new BufferedReader(new FileReader(new File(filename)))) {
            return in.lines().skip(1).collect(Collectors.toList());
        } catch (FileNotFoundException ex){
            System.out.println("file not found");
            return null;
        }
    }

    public static List<String> convertToJson( List<String> inputLogs) throws ParseException {
        List<String> jsons = new ArrayList<>();
        DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
        inputLogs.forEach(line -> {
            String[] x = line.split(", ");
            try {
                Date date = format.parse(x[2]);
                jsons.add((new Log(x[0], x[1], date, Integer.parseInt(x[3]))).convertToJson());
            } catch (ParseException e){
                System.out.println("Parse exeption: "+ e);
            }
        });
        return jsons;
    }

    public static void write(List<String> jsons, String filename){
        try {
            FileWriter writer = new FileWriter(filename);
            for(String str: jsons) {
                writer.write(str + "\n");
            }
            writer.close();
        } catch (IOException ex){
            System.out.println("createDirectory failed:");
        }
    }
}
