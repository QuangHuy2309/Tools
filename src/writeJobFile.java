import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;

public class writeJobFile {
    final static String startDate = "2023-01-01 01:00:00";
    final static String endDate = "2023-06-11 01:00:00";
    final static String lastGenerated ="2022-06-12 01:00:00";
    final static int newRollingWindow = 138;
    final static int newChunkingWindow = 7;


    public static ArrayList<String> readFile(){
        System.out.println("*** STEP 1 : READ LIST ACCOUNT FILE ***");
        var listAccount = new ArrayList<String>();
        try {
            File    myObj    = new File("src/resources/input/listAccount.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                listAccount.add(data);
            }
            myReader.close();
            System.out.println("Success!!!");
            System.out.println("Number of AdAccount: "+listAccount.size());
            return listAccount;
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
            return listAccount;
        }
    }

    public static ArrayList<String> createScript(ArrayList<String> listAccount){
        System.out.println("*** STEP 2 : CREATE SCRIPT ***");
        var jobLists = new ArrayList<String>();
        for (String account :listAccount){
            String job = String.format("INSERT INTO datacollector.jobs (collector, created,\tduration, duration_unit, `end`,\textra_parameters, job_status, last_generated_end, lookback,\tlookback_unit, modified, network, `start`, configuration, collection_type, spacing_time, spacing_duration, end_offset_days, comments, timezone,\tcreated_by,\tmodified_by) VALUES( '', NOW(), 1, 'DAYS', '%s', '{\"newRollingWindowDays\":%d,\"rollingWindowChunkingDays\":%d,\"is_unified\":true,\"property_name\": \"account_id\", \"traceKey\":\"account_id,accountName\", \"traceAsName\":{\"account_id\":\"accountId\"}, \"filter\":[{\"property_type\":\"AdAccount\", \"property_id\":\"%s\"}]}', 'PAUSED', '%s', 0, 'DAYS', NOW(), 'snapchat', '%s', '{ \"generator\": \"com.unified.platform.collectionengine.generator.EntityToCredentialGenerator\", \"entity_type\": \"AdAccount\", \"credential_type\": \"ApplicationToken\", \"content_type\": \"Paid\", \"call_stack_id\": \"snapchat:v1:paid:stats\"}', 'backfill.historical', NULL, NULL, 0, 'PS-3288 Horizon snapchat backfill', 'UTC', NULL, NULL);",endDate,newRollingWindow,newChunkingWindow,account.trim(),lastGenerated,startDate);
            jobLists.add(job);
        }
        System.out.println("Success!!!");
        return jobLists;
    }

    public static void writeFile(ArrayList<String> listAccount) throws IOException {
        System.out.println("*** STEP 3 : WRITE FILE ***");
        String[] start = startDate.split("\\s");
        String[] end = endDate.split("\\s");
        Path file = Paths.get("src/resources/output/PS-3288_Snapchat_backfill"+start[0]+"_"+end[0]+".sql");
        Files.write(file, listAccount, StandardCharsets.UTF_8);
        System.out.println("Success!!!");
    }
    public static void main (String[] args) throws IOException {
        System.out.println("Start programs!!!");
        var listAccount = readFile();
        writeFile(createScript(listAccount));
    }
}
