package com.amazonaws.lambda.demo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.CopyObjectRequest;

public class ETL implements RequestHandler<S3Event, String> {
	
	//configuration section
	static String OUTFILE_FILE_NAME = System.getenv("OUTFILE_FILE_NAME");

	//handles by copying to a new name in the same bucket
	@Override
    public String handleRequest(S3Event s3event, Context context) {
    	
		//copy activity
		S3EventNotificationRecord record = s3event.getRecords().get(0);

        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getKey().replace('+', ' '); // Object key may have spaces or UNICODE non-ASCII characters.
        try {
			srcKey = URLDecoder.decode(srcKey, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return "failure - " + e.getMessage();
		}

        String object_key = srcKey;
        String dest_key = OUTFILE_FILE_NAME;
        String from_bucket = srcBucket;

        System.out.format("Copying object %s to same bucket %s with different Name as %s\n",object_key, from_bucket, dest_key);
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
        	CopyObjectRequest copyObjRequest = new CopyObjectRequest(from_bucket, object_key, from_bucket, dest_key);
        	s3.copyObject(copyObjRequest);
            System.out.println("after copy");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        	return "failure - " + e.getMessage();
        }
        System.out.format("Successfully copied object %s to different name %s in the same bucket %s\n",object_key, dest_key, from_bucket);
        //end of copy activity
        
        return "success";
    }
}
