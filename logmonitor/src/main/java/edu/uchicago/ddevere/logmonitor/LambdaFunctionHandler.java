package edu.uchicago.ddevere.logmonitor;

import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    
    
    public LambdaFunctionHandler() {}

    // Test purpose only.
//    LambdaFunctionHandler(AmazonS3 s3) {
//        this.s3 = s3;
//    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        for(S3EventNotificationRecord record : event.getRecords()) {
        	String bucket = record.getS3().getBucket().getName();
        	String key = record.getS3().getObject().getKey();
        	try {
        		context.getLogger().log("About to download from bucket: " + bucket + " key: " + key);
        		S3Object object = s3.getObject(new GetObjectRequest(bucket, key));
        		context.getLogger().log("Downloaded file");
        		S3ObjectInputStream inputStream = object.getObjectContent();
        		GZIPInputStream gzip = new GZIPInputStream(inputStream);
        		context.getLogger().log("Unzipping and parsing file");
        		CSVParser parser = CSVParser.parse(new InputStreamReader(gzip), CSVFormat.newFormat(' ').withFirstRecordAsHeader());
        		List<Row> rows = new ArrayList<>();
        		for(CSVRecord csvRecord : parser) {
        			if(csvRecord.get("log-status").equals("NODATA")) {
        				continue;
        			}
        			List<Row> found = expandToByMinute(context, csvRecord);
        			if(found != null) {
        				rows.addAll(found);
        			} else {
        				context.getLogger().log("Didn't get any rows");
        			}
        		}
        		context.getLogger().log("parsed: " + rows.size() + " rows");
        		sendToHbase(context, rows, System.getenv("hbase_host") + "/" + System.getenv("hbase_table") + "/batch");
        	} catch(Exception e) {
        		context.getLogger().log("Error in handleRequest: " + e.toString());
        	}
        }
        context.getLogger().log("done");
        return "done";
    }
    
    private void sendToHbase(Context context, List<Row> rows, String uri) {
    	try {
    		HttpClient client = HttpClientBuilder.create().build();
    		context.getLogger().log("Sending post request to: " + uri);
    		HttpPost request = new HttpPost(uri);
        	request.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(rows), ContentType.APPLICATION_JSON));
        	HttpResponse response = client.execute(request);
        	if(response.getStatusLine().getStatusCode() == 200) {
        		context.getLogger().log("Successfully saved row to hbase: " + rows.toString());
        	} else {
        		context.getLogger().log("Error code when saving row: " + response.getStatusLine().getStatusCode());
        	}
    	} catch(Exception e) {
    		context.getLogger().log("Error in sendtoHbase: " + e.toString());
    	}
    	
    }
    
    private List<Row> expandToByMinute(Context context, CSVRecord record) {
    	try {
    		Row src = new Row();
        	Row dst = new Row();
        	src.setName(record.get("srcaddr"));
        	dst.setName(record.get("dstaddr"));
        	List<Column> srcColumns = new ArrayList<>();
        	List<Column> dstColumns = new ArrayList<>();
        	BigDecimal startMinute = new BigDecimal(record.get("start")).divide(new BigDecimal("60"), RoundingMode.HALF_UP);
        	BigDecimal stopMinute = new BigDecimal(record.get("end")).divide(new BigDecimal("60"), RoundingMode.HALF_UP);
        	BigDecimal range = stopMinute.subtract(startMinute);
        	if(range.compareTo(BigDecimal.ZERO) == 0) {
        		srcColumns.add(new Column(
        				"outBytes", 
        				startMinute.toPlainString(), 
        				null, 
        				record.get("bytes")));
        		srcColumns.add(new Column(
        				"outCount", 
        				startMinute.toPlainString(),  
        				null, 
        				"1"));
        		dstColumns.add(new Column(
        				"inBytes",
        				startMinute.toPlainString(), 
        				null,
        				record.get("bytes")));
        		dstColumns.add(new Column(
        				"inCount",
        				startMinute.toPlainString(), 
        				null,
        				"1"));
        	} else {
        		for(int i = startMinute.intValue(); i < stopMinute.intValue(); i++) {
            		srcColumns.add(new Column(
            				"outBytes", 
            				String.valueOf(i), 
            				null, 
            				new BigDecimal(record.get("bytes")).divide(range, RoundingMode.HALF_UP).toPlainString()));
            		srcColumns.add(new Column(
            				"outCount", 
            				String.valueOf(i), 
            				null, 
            				"1"));
            		dstColumns.add(new Column(
            				"inBytes",
            				String.valueOf(i),
            				null,
            				new BigDecimal(record.get("bytes")).divide(range, RoundingMode.HALF_UP).toPlainString()));
            		dstColumns.add(new Column(
            				"inCount",
            				String.valueOf(i),
            				null,
            				"1"));
            	}
        	}
        	
        	src.setColumns(srcColumns);
        	dst.setColumns(dstColumns);
        	return Arrays.asList(src, dst);
    	} catch(Exception e) {
    		context.getLogger().log("Error in expandToByMinute: " + e.toString());
    		return null;
    	}
    	
    }
}