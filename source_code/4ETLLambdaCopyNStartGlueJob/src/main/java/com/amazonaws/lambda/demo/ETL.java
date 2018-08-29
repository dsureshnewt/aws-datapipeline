package com.amazonaws.lambda.demo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.StartJobRunRequest;
import com.amazonaws.services.glue.model.StartJobRunResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

public class ETL implements RequestHandler<S3Event, String> {
	
	//configuration section
	static String STAGING_S3_BUCKET_NAME = System.getenv("STAGING_S3_BUCKET_NAME");
	static String GLUE_JOB_NAME = System.getenv("GLUE_JOB_NAME");
	static String SPARK_JOB_INPUT_S3_BUCKET_URI = System.getenv("SPARK_JOB_INPUT_S3_BUCKET_URI");
	static String SPARK_JOB_OUTPUT_S3_BUCKET_URI = System.getenv("SPARK_JOB_OUTPUT_S3_BUCKET_URI");
	static String GLUE_DATABASE_NAME = System.getenv("GLUE_DATABASE_NAME");
	static String GLUE_SOURCE_TABLE_NAME = System.getenv("GLUE_SOURCE_TABLE_NAME");
	static String GLUE_DESTINATION_TABLE_NAME = System.getenv("GLUE_DESTINATION_TABLE_NAME");

	//handles by copying input file to staging directory and start Glue job to do the transformation and save the result in RDS
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
        String from_bucket = srcBucket;
        String to_bucket = STAGING_S3_BUCKET_NAME;

        System.out.format("Copying object %s from bucket %s to %s\n",object_key, from_bucket, to_bucket);
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            s3.copyObject(from_bucket, object_key, to_bucket, object_key);
        } catch (AmazonServiceException e) {
            return "failure - " + e.getErrorMessage();
        }
        System.out.format("Successfully copied object %s from bucket %s to %s\n",object_key, from_bucket, to_bucket);
        //end of copy activity
        
        //activate glue job
        AWSGlue glue =  AWSGlueClientBuilder.defaultClient();
        StartJobRunRequest jobRunRequest = new StartJobRunRequest();
        jobRunRequest.setJobName(GLUE_JOB_NAME);
        jobRunRequest.addArgumentsEntry("--SPARK_JOB_INPUT_S3_BUCKET_URI", SPARK_JOB_INPUT_S3_BUCKET_URI);
        jobRunRequest.addArgumentsEntry("--SPARK_JOB_OUTPUT_S3_BUCKET_URI", SPARK_JOB_OUTPUT_S3_BUCKET_URI);
        jobRunRequest.addArgumentsEntry("--GLUE_DATABASE_NAME", GLUE_DATABASE_NAME);
        jobRunRequest.addArgumentsEntry("--GLUE_SOURCE_TABLE_NAME", GLUE_SOURCE_TABLE_NAME);
        jobRunRequest.addArgumentsEntry("--GLUE_DESTINATION_TABLE_NAME", GLUE_DESTINATION_TABLE_NAME);
        StartJobRunResult jobRunResult = glue.startJobRun(jobRunRequest);
        context.getLogger().log("Activated glue job..");
    	context.getLogger().log("activateGlueJobRes: " + jobRunResult);
        //end of glue job activation
        
        return "success";
    }

}
