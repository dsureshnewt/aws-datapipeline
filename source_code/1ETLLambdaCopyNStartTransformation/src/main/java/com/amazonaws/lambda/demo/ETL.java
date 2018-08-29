package com.amazonaws.lambda.demo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.datapipeline.DataPipeline;
import com.amazonaws.services.datapipeline.DataPipelineClientBuilder;
import com.amazonaws.services.datapipeline.model.ActivatePipelineRequest;
import com.amazonaws.services.datapipeline.model.ActivatePipelineResult;
import com.amazonaws.services.datapipeline.model.CreatePipelineRequest;
import com.amazonaws.services.datapipeline.model.CreatePipelineResult;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

public class ETL implements RequestHandler<S3Event, String> {

	//configuration section
	static String STAGING_S3_BUCKET_NAME = System.getenv("STAGING_S3_BUCKET_NAME");
	static String MYSQL_LIB_S3_BUCKET_URI = System.getenv("MYSQL_LIB_S3_BUCKET_URI");
	static String SPARK_JOB_S3_BUCKET_URI = System.getenv("SPARK_JOB_S3_BUCKET_URI");
	static String SPARK_JOB_INPUT_S3_BUCKET_URI = System.getenv("SPARK_JOB_INPUT_S3_BUCKET_URI");
	static String SPARK_JOB_OUTPUT_S3_BUCKET_URI = System.getenv("SPARK_JOB_OUTPUT_S3_BUCKET_URI");
	static String LOG_S3_BUCKET_URI = System.getenv("LOG_S3_BUCKET_URI");
	static String JDBC_URL = System.getenv("JDBC_URL");
	static String DB_TABLE = System.getenv("DB_TABLE");
	static String DB_USER = System.getenv("DB_USER");
	static String DB_PASSWORD = System.getenv("DB_PASSWORD");
	static String DB_DRIVER = System.getenv("DB_DRIVER");
	
	//handles by copying input file to staging directory and trigger data pipeline to start applying transformation
	@Override
    public String handleRequest(S3Event s3event, Context context) {
    	
		//copy activity
		S3EventNotificationRecord record = s3event.getRecords().get(0);

        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getKey().replace('+', ' '); //Object key may have spaces or UNICODE non-ASCII characters.
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
        
        //activate data pipeline
        DataPipeline dp =  DataPipelineClientBuilder.defaultClient();
        
        String pipelineName = "aws-etl-pipeline-job-"+System.currentTimeMillis();
    	CreatePipelineRequest createPipelineRequest = new CreatePipelineRequest().withName(pipelineName).withUniqueId(pipelineName);
    	CreatePipelineResult createPipelineResult = dp.createPipeline(createPipelineRequest);
    	String pipelineId = createPipelineResult.getPipelineId();
        
    	PipelineObject emrCluster = new PipelineObject()
    	    .withName("EmrClusterObj")
    	    .withId("EmrClusterObj")
    	    .withFields(
    	    	new Field().withKey("resourceRole").withStringValue("EC2_ROLE_FOR_DATA_PIPELINE"),
    			new Field().withKey("role").withStringValue("DataPipelineDefaultRole"),
    			new Field().withKey("releaseLabel").withStringValue("emr-5.15.0"),
    			new Field().withKey("type").withStringValue("EmrCluster"),
    			new Field().withKey("terminateAfter").withStringValue("30 Minutes"),
    			new Field().withKey("applications").withStringValue("Spark")
    			);
      
    	String stepStrVal = "command-runner.jar,spark-submit,--deploy-mode,cluster,--jars,"
    						+ MYSQL_LIB_S3_BUCKET_URI
    						+ ",--class,com.etl.spark.Demo,"
    						+ SPARK_JOB_S3_BUCKET_URI
    						+ ","
    						+ SPARK_JOB_INPUT_S3_BUCKET_URI
    						+ ","
    						+ SPARK_JOB_OUTPUT_S3_BUCKET_URI
    						+ ","
    						+ JDBC_URL
    						+ ","
    						+ DB_TABLE
    						+ ","
    						+ DB_USER
    						+ ","
    						+ DB_PASSWORD
    						+ ","
    						+ DB_DRIVER;
    	
    	PipelineObject emrActivity = new PipelineObject()
    	    .withName("EmrActivityObj")
    	    .withId("EmrActivityObj")
    	    .withFields(
    			new Field().withKey("step").withStringValue(stepStrVal),
    			new Field().withKey("runsOn").withRefValue("EmrClusterObj"),
    			new Field().withKey("type").withStringValue("EmrActivity")
    			);
    	
    	PipelineObject defaultObject = new PipelineObject()
    		    .withName("Default")
    		    .withId("Default")
    		    .withFields(
    				new Field().withKey("failureAndRerunMode").withStringValue("CASCADE"),
    				new Field().withKey("resourceRole").withStringValue("EC2_ROLE_FOR_DATA_PIPELINE"),
    				new Field().withKey("role").withStringValue("DataPipelineDefaultRole"),
    				new Field().withKey("pipelineLogUri").withStringValue(LOG_S3_BUCKET_URI),
    				new Field().withKey("scheduleType").withStringValue("ONDEMAND")
    				);
    	
    	List<PipelineObject> pipelineObjects = new ArrayList<PipelineObject>();
        
    	pipelineObjects.add(emrActivity);
    	pipelineObjects.add(emrCluster);
    	pipelineObjects.add(defaultObject);
        
    	PutPipelineDefinitionRequest putPipelineDefintion = new PutPipelineDefinitionRequest()
    	    .withPipelineId(pipelineId)
    	    .withPipelineObjects(pipelineObjects);
    	
    	PutPipelineDefinitionResult putPipelineResult = dp.putPipelineDefinition(putPipelineDefintion);
    	context.getLogger().log("putPipelineResult: " + putPipelineResult);
    	context.getLogger().log("Activating pipeline...");
    	ActivatePipelineRequest activatePipelineReq = new ActivatePipelineRequest()
    		    .withPipelineId(pipelineId);
    	ActivatePipelineResult activatePipelineRes = dp.activatePipeline(activatePipelineReq);
    	context.getLogger().log("Activated pipeline.");
    	context.getLogger().log("activatePipelineRes: " + activatePipelineRes);
    	context.getLogger().log("pipelineId: " + pipelineId);
        //end of data pipeline activation
        
        return "success";
    }
}
