package edu.uchicago.ddevere.logmonitor;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.params.HttpParams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification.S3BucketEntity;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
@RunWith(MockitoJUnitRunner.class)
public class LambdaFunctionHandlerTest {

    private final String CONTENT_TYPE = "image/jpeg";
    private S3Event event;

    @Mock
    private AmazonS3 s3Client;
    @Mock
    private S3Object s3Object;

    @Captor
    private ArgumentCaptor<GetObjectRequest> getObjectRequest;

    @Before
    public void setUp() throws IOException {
    	S3EventNotificationRecord record = Mockito.mock(S3EventNotificationRecord.class);
    	S3Entity entity = new S3Entity("", new S3BucketEntity("dan-flow-logs", null, ""), new S3ObjectEntity("AWSLogs/429810162652/vpcflowlogs/us-east-1/2018/12/09/429810162652_vpcflowlogs_us-east-1_fl-07d36cb659fc28602_20181209T2135Z_b0d11380.log.gz", 0L, null, null, null), "1");
    	when(record.getS3()).thenReturn(entity);
        event = new S3Event(Arrays.asList(record));
        //TestUtils.parse("/s3-event.put.json", S3Event.class);

        // TODO: customize your mock logic for s3 client
//        S3ObjectInputStream inputStream = new S3ObjectInputStream(this.getClass().getResourceAsStream("/testlog.gz"), null);
//        when(s3Object.getObjectContent()).thenReturn(inputStream);
//        when(s3Client.getObject(getObjectRequest.capture())).thenReturn(s3Object);
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    public void testLambdaFunctionHandler() {
    	HttpClient httpClient = Mockito.mock(HttpClient.class, new Answer<HttpResponse>() {

			@Override
			public HttpResponse answer(InvocationOnMock invocation) throws Throwable {
				HttpPost post = (HttpPost)invocation.getArgument(0);
				System.out.println(IOUtils.toString(post.getEntity().getContent()));
				return new HttpResponse() {
					
					@Override
					public void setParams(HttpParams params) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setHeaders(Header[] headers) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setHeader(String name, String value) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setHeader(Header header) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void removeHeaders(String name) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void removeHeader(Header header) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public HeaderIterator headerIterator(String name) {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public HeaderIterator headerIterator() {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public ProtocolVersion getProtocolVersion() {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public HttpParams getParams() {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public Header getLastHeader(String name) {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public Header[] getHeaders(String name) {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public Header getFirstHeader(String name) {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public Header[] getAllHeaders() {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public boolean containsHeader(String name) {
						// TODO Auto-generated method stub
						return false;
					}
					
					@Override
					public void addHeader(String name, String value) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void addHeader(Header header) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setStatusLine(ProtocolVersion ver, int code, String reason) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setStatusLine(ProtocolVersion ver, int code) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setStatusLine(StatusLine statusline) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setStatusCode(int code) throws IllegalStateException {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setReasonPhrase(String reason) throws IllegalStateException {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setLocale(Locale loc) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void setEntity(HttpEntity entity) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public StatusLine getStatusLine() {
						return new StatusLine() {
							
							@Override
							public int getStatusCode() {
								// TODO Auto-generated method stub
								return 200;
							}
							
							@Override
							public String getReasonPhrase() {
								// TODO Auto-generated method stub
								return null;
							}
							
							@Override
							public ProtocolVersion getProtocolVersion() {
								// TODO Auto-generated method stub
								return null;
							}
						};
					}
					
					@Override
					public Locale getLocale() {
						// TODO Auto-generated method stub
						return null;
					}
					
					@Override
					public HttpEntity getEntity() {
						// TODO Auto-generated method stub
						return null;
					}
				};
			}
		});
        LambdaFunctionHandler handler = new LambdaFunctionHandler();
        Context ctx = createContext();

        String output = handler.handleRequest(event, ctx);

        // TODO: validate output here if needed.
        Assert.assertEquals("done", output);
    }
}
