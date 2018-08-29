package com.amazonaws.lambda.demo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

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
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

public class ETL implements RequestHandler<S3Event, String> {
	
	//configuration section
	static String MYSQL_LIB_S3_BUCKET_URI = System.getenv("MYSQL_LIB_S3_BUCKET_URI");
	static String DB_NAME = System.getenv("DB_NAME");
	static String DB_TABLE = System.getenv("DB_TABLE");
	static String DB_USER = System.getenv("DB_USER");
	static String DB_PASSWORD = System.getenv("DB_PASSWORD");
	static String DB_REGION = System.getenv("DB_REGION");
	static String DB_RDS_INSTANCE_ID = System.getenv("DB_RDS_INSTANCE_ID");
	static String LOG_S3_BUCKET_URI = System.getenv("LOG_S3_BUCKET_URI");

	//handles by triggering the data pipeline to copy the spark job output to RDS MySQL table
	@Override
    public String handleRequest(S3Event s3event, Context context) {
    	
		S3EventNotificationRecord record = s3event.getRecords().get(0);

        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getKey().replace('+', ' '); // Object key may have spaces or unicode non-ASCII characters.
        try {
			srcKey = URLDecoder.decode(srcKey, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return "failure - " + e.getMessage();
		}

        String object_key = srcKey;
        String from_bucket = srcBucket;
        String s3DataFilePath = "s3://"+from_bucket+"/"+object_key;
        
        //activate data pipeline
        DataPipeline dp =  DataPipelineClientBuilder.defaultClient();
        
        String pipelineName = "aws-etl-pipeline-job-copy-s3-to-rds"+System.currentTimeMillis();
    	CreatePipelineRequest createPipelineRequest = new CreatePipelineRequest().withName(pipelineName).withUniqueId(pipelineName);
    	CreatePipelineResult createPipelineResult = dp.createPipeline(createPipelineRequest);
    	String pipelineId = createPipelineResult.getPipelineId();
        
    	PipelineObject ec2Resource = new PipelineObject()
    	    .withName("Ec2ResourceObj")
    	    .withId("Ec2ResourceObj")
    	    .withFields(
    	    	new Field().withKey("resourceRole").withStringValue("EC2_ROLE_FOR_DATA_PIPELINE"),
    			new Field().withKey("role").withStringValue("DataPipelineDefaultRole"),
    			new Field().withKey("actionOnResourceFailure").withStringValue("retryAll"),
    			new Field().withKey("actionOnTaskFailure").withStringValue("terminate"),
    			new Field().withKey("type").withStringValue("Ec2Resource"),
    			new Field().withKey("terminateAfter").withStringValue("30 Minutes"),
    			new Field().withKey("maximumRetries").withStringValue("1"),
    			new Field().withKey("instanceType").withStringValue("m1.medium")
    			);
      
    	PipelineObject s3DataNode = new PipelineObject()
        	    .withName("S3DataNodeObj")
        	    .withId("S3DataNodeObj")
        	    .withFields(
        			new Field().withKey("filePath").withStringValue(s3DataFilePath),
        			new Field().withKey("type").withStringValue("S3DataNode")
        			);
    	
    	PipelineObject rdsDatabase = new PipelineObject()
        	    .withName("RdsDatabaseObj")
        	    .withId("RdsDatabaseObj")
        	    .withFields(
        			new Field().withKey("databaseName").withStringValue(DB_NAME),
        			new Field().withKey("type").withStringValue("RdsDatabase"),
        			new Field().withKey("username").withStringValue(DB_USER),
        			new Field().withKey("*password").withStringValue(DB_PASSWORD),
        			new Field().withKey("jdbcDriverJarUri").withStringValue(MYSQL_LIB_S3_BUCKET_URI),
        			new Field().withKey("region").withStringValue(DB_REGION),
        			new Field().withKey("rdsInstanceId").withStringValue(DB_RDS_INSTANCE_ID)
        			);
    	
    	PipelineObject sqlDataNode = new PipelineObject()
        	    .withName("SqlDataNodeObj")
        	    .withId("SqlDataNodeObj")
        	    .withFields(
        			new Field().withKey("database").withRefValue("RdsDatabaseObj"),
        			new Field().withKey("type").withStringValue("SqlDataNode"),
        			new Field().withKey("insertQuery").withStringValue("INSERT INTO #{table} (id,firstname,lastname,email,phone,salary,designation,manager,manageremail,department,date,isTailgated,isAbsent) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `firstname`=VALUES(`firstname`),`lastname`=VALUES(`lastname`),`email`=VALUES(`email`),`phone`=VALUES(`phone`),`salary`=VALUES(`salary`),`designation`=VALUES(`designation`),`manager`=VALUES(`manager`),`manageremail`=VALUES(`manageremail`),`department`=VALUES(`department`),`isTailgated`=VALUES(`isTailgated`),`isAbsent`=VALUES(`isAbsent`);"),
        			new Field().withKey("table").withStringValue(DB_TABLE)
        			);
    	
    	PipelineObject copyActivity = new PipelineObject()
    	    .withName("CopyActivityObj")
    	    .withId("CopyActivityObj")
    	    .withFields(
    			new Field().withKey("input").withRefValue("S3DataNodeObj"),
    			new Field().withKey("output").withRefValue("SqlDataNodeObj"),
    			new Field().withKey("type").withStringValue("CopyActivity"),
    			new Field().withKey("runsOn").withRefValue("Ec2ResourceObj")
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
        
    	pipelineObjects.add(defaultObject);
    	pipelineObjects.add(ec2Resource);
    	pipelineObjects.add(s3DataNode);
    	pipelineObjects.add(copyActivity);
    	pipelineObjects.add(rdsDatabase);
    	pipelineObjects.add(sqlDataNode);
        
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
