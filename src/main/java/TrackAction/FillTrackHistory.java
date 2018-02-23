package TrackAction;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class FillTrackHistory {
    String[] arguments=null;
    public FillTrackHistory(String[] args) {
        arguments=args;
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found ";
    }

    private String parseSQL(String sql) {
        String result=sql;
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals("--from_mongodb_users"))result=result.replace("{from_mongodb_users}",arguments[i+1]);
            if(arguments[i].equals("--artist_track_images"))result=result.replace("{artist_track_images}",arguments[i+1]);
            if(arguments[i].equals("--playlist_geography"))result=result.replace("{playlist_geography}",arguments[i+1]);
            if(arguments[i].equals("--product"))result=result.replace("{product}",arguments[i+1]);
            if(arguments[i].equals("--playlist_track_history"))result=result.replace("{playlist_track_history}",arguments[i+1]);
            if(arguments[i].equals("--playlist_history"))result=result.replace("{playlist_history}",arguments[i+1]);
            if(arguments[i].equals("--streams"))result=result.replace("{streams}",arguments[i+1]);
            if(arguments[i].equals("--canopus_resource"))result=result.replace("{canopus_resource}",arguments[i+1]);
            if(arguments[i].equals("--playlist_track_action"))result=result.replace("{playlist_track_action}",arguments[i+1]);
            //if(arguments[i].equals("--executionDate"))result=result.replace("{ExecutionDate}",arguments[i+1]);
        }
        return result;
    }

    private String getSQL(String executionDate) {
        try {
            InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream("insert2.sql");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            StringBuffer stringBuffer = new StringBuffer();
            String line = null;
            while ((line = reader.readLine()) != null) {
                stringBuffer.append(line).append("\n");
            }
            String sql =  stringBuffer.toString();
            String fullsql = parseSQL(sql);
            fullsql = fullsql.replace("{ExecutionDate}",executionDate);
            return fullsql;
        } catch(IOException e) {
            System.out.println("IOException");
            return null;
        }
    }

    private String getQuery() {
        String q="";
        q+="SELECT source_uri as playlist_uri, COUNT(source_uri) as streams";
        q+=" FROM `{streams}`";
        q+=" WHERE LENGTH(source_uri) > 0 AND _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 * 24 HOUR),DAY) AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY)";
        q+=" GROUP BY source_uri";
        q+=" ORDER BY streams DESC";
        q+=" LIMIT 10000";
        return q;
    }

    public void findTrackAPI(String executionDate) throws IOException {
        DateDaysAgo dda = new DateDaysAgo();
        String actualDate=dda.getDaysAgo(executionDate,-1);
        String sql  =getSQL(executionDate);//.replace("{ExecutionDate}",executionDate).replace("{project}",project);
        String[] args={"--project="+getArgument("--project"),"--runner="+getArgument("--runner")};
        String exec_sql = "SELECT * from get_top_playlists10000";
        String total_query=sql+"\n"+exec_sql;
        total_query = getQuery().replace("{streams}",getArgument("--streams"));
        //System.out.println(sql+"\n"+exec_sql);
        String millis = ""+System.currentTimeMillis();
        String index = millis.substring(millis.length()-3);
        String output = getArgument("--outputfile").replace("{actualDate}",actualDate);
        //String output = outputfile+"/trackHistoryAPI_"+actualDate+"/track_history_"+actualDate+".csv";
        //inputFile=getArgument("--input_file").replace("{actualDate}",actualDate);
        System.out.println("Output file="+output);
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation(getArgument("--temp_directory"));
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        BigQueryIO.Read readBigQuery = BigQueryIO.read().fromQuery(total_query).usingStandardSql();
        pipeline.apply(readBigQuery).apply(ParDo.of(new ParDoTrackHistory(actualDate)))
                .apply(TextIO.write().to(output));
        pipeline.run().waitUntilFinish();
    }

    private String getString() {
        try {
            InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream("api_response.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            StringBuffer stringBuffer = new StringBuffer();
            String line = null;
            while ((line = reader.readLine()) != null) {
                stringBuffer.append(line).append("\n");
            }
            return stringBuffer.toString();
        } catch(IOException e) {
            System.out.println("IOException");
            return null;
        }
    }

    public String getDaysAgo(int daysago) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, daysago);
        Date newDate = cal.getTime();
        String newDateStr = dateFormat.format(newDate);
        return newDateStr;
    }

    public static void main(String[] args) throws Exception {
        FillTrackHistory fth = new FillTrackHistory(args);
        DateDaysAgo dda = new DateDaysAgo();
        String executionDate=dda.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("--executionDate")) executionDate=args[i+1];
        }
        fth.findTrackAPI(executionDate);
    }
}
