package TrackAction;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.api.services.bigquery.model.TableRow;

public class LoadGCStoBQ {
    String executionDate="";
    String actualDate="";
    String project="";
    String dataSet="";
    String destinationTable="";
    String tempDirectory="";
    String inputFile="";
    String runner="DirectRunner";
    String[] arguments=null;
    TableSchema schema=null;
    ArrayList<String> schemaStructure=null;
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();


    public String getDaysAgo(String day, int daysago) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        if (!day.equals("Not found ")) {
            date = dateFormat.parse(day);
        }
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, daysago);
        Date newDate = cal.getTime();
        String newDateStr = dateFormat.format(newDate);
        return newDateStr;
    }

    public LoadGCStoBQ(String[] args, ArrayList<String> schemafields) throws Exception {
        arguments=args;
        String[] destTable=getArgument("--destination_table").split("\\.");
        project=destTable[0];
        dataSet=destTable[1];
        destinationTable=destTable[2];
        tempDirectory=getArgument("--temp_directory");
        runner=getArgument("--runner");
        executionDate = getArgument("--executionDate");
        actualDate = getDaysAgo(executionDate,-1);
        inputFile=getArgument("--input_file").replace("{actualDate}",actualDate);
        ReadFileLine rfl = new ReadFileLine();
        schemaStructure=rfl.readFileLine(getArgument("--schema"));
        System.out.println("inputFile="+inputFile);
        System.out.println("executionDate="+executionDate);
        System.out.println("actualDate="+actualDate);
        System.out.println("destinationTable="+ project+"."+dataSet+"."+destinationTable);
        List<TableFieldSchema> fields = new ArrayList<>();
        for(int i=0; i<schemaStructure.size();i++) {
            String s = schemaStructure.get(i);
            String[] ss  = s.split(":");
            fields.add(new TableFieldSchema().setName(ss[0]).setType(ss[1]));
        }
        schema = new TableSchema().setFields(fields);
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found ";
    }

    public boolean checkTableExists() throws Exception{
        String exec_sql="SELECT count(1) as count FROM `"+project+"."+dataSet+".__TABLES_SUMMARY__` WHERE table_id = '"+destinationTable+"'";
        //System.out.println(" project="+project+"  ds="+dataSet+"  tableName="+tableName);
        System.out.println(exec_sql);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(exec_sql)
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        // Wait for the query to complete.
        queryJob = queryJob.waitFor();
        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        // Get the results.
        QueryResponse response = bigquery.getQueryResults(jobId);
        QueryResult qresult = response.getResult();
        // Print all pages of the results.
        ArrayList<String> output = new ArrayList<String>();
        String result = "";
        while (qresult != null) {
            for (List<FieldValue> row : qresult.iterateAll()) {
                String fn0 = row.get(0).getValue().toString();
                long fv0 = row.get(0).getLongValue();

                System.out.println( "number of records="+fv0+" fn0="+fn0);
                if(fv0==0) {
                    //System.out.println(" table="+project+"."+dataSet+"."+tableName+" does not exist");
                    return false;
                } else {
                    //System.out.println(" table="+project+"."+dataSet+"."+tableName+" exists");
                    return true;
                }
            }
            qresult = qresult.getNextPage();
        }

        return false;
    }

    void createTable() throws Exception{
        TableId tableId = null;
        if (checkTableExists()) {
            System.out.println("Table "+destinationTable+" already exists, exiting...");
            return;
        }
        if (StringUtils.isNotEmpty(project)) {
            tableId = TableId.of(project, dataSet, destinationTable);
        } else {
            tableId = TableId.of(dataSet, destinationTable);
        }
/*
        List<Field> fields = new ArrayList<>();
        fields.add(Field.of("transaction_date", Field.Type.timestamp()));
        fields.add(Field.of("product_id", Field.Type.string()));
        fields.add(Field.of("sales_country_code", Field.Type.string()));
        Schema schema = Schema.of(fields);
*/
        StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                ;

        TableDefinition tableDefinition = builder.build();
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
        System.out.println("Table created");
    }

    public void execute(ArrayList<String> schemafields) throws Exception {
        //List<TableFieldSchema> fields = new ArrayList<>();
        //for(int i=0; i<schemafields.size();i++) {
        //    String s = schemafields.get(i);
        //    String[] ss  = s.split(":");
        //    fields.add(new TableFieldSchema().setName(ss[0]).setType(ss[1]));
        //}
        //TableSchema schema = new TableSchema().setFields(fields);

        createTable();
        System.out.println("Start process");

        // Create a PipelineOptions object.
        String[] args = {"--project="+project,"--runner="+runner};
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        options.setTempLocation(tempDirectory);
        //options.setRunner(DataflowRunner.class);

        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);
        //inputFile="gs://umg-dev/swift_alerts/trackHistoryAPI_2017-11-30/track_history_2017-11-30.csv*";
        //inputFile="gs://umg-alexey-test/source-directory/streams.gz";
        System.out.println("Input fs: "+inputFile);
        TextIO.Read readFile = TextIO.read().from(inputFile);
        String destination_table=project+":"+dataSet+"."+destinationTable+"$"+actualDate.replace("-","");
        //destination_table=project+":"+dataSet+"."+destinationTable;
        BigQueryIO.Write<TableRow> writeBigQuery = BigQueryIO.writeTableRows()
                .to(destination_table)
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

        p.apply(readFile)
                .apply(ParDo.of(new ParDoStringParser(schemaStructure))) //schemafields
                .apply(writeBigQuery)
        ;

        p.run().waitUntilFinish();
        System.out.println("End of process");
    }

    public static void main(String args[]) throws Exception{
        ArrayList<String> sb = new ArrayList<String>();
        sb.add("playlist_uri:STRING");
        sb.add("playlist_date:DATE");
        sb.add("playlist_id:STRING");
        sb.add("playlist_owner:STRING");
        sb.add("playlist_owner_uri:STRING");
        sb.add("playlist_name:STRING");
        sb.add("followers:INTEGER");
        sb.add("isrc:STRING");
        sb.add("track_add_by:STRING");
        sb.add("track_uri:STRING");
        sb.add("track_name:STRING");
        sb.add("artist_uri:STRING");
        sb.add("artist_name:STRING");
        sb.add("album_uri:STRING");
        sb.add("album_name:STRING");
        sb.add("position:INTEGER");
        sb.add("load_timestamp:TIMESTAMP");
        LoadGCStoBQ loadgcs2bq = new LoadGCStoBQ(args,sb);
        long start=System.currentTimeMillis();
        loadgcs2bq.execute(sb);
        System.out.println("End of process "+(System.currentTimeMillis()-start)/1000+" sec");
    }
}
