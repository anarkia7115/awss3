package com.gcbi.hadoop.awss3;

import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
//import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class S3Upload {

    public static void main(String[] args) throws Exception {
        String existingBucketName = "gcbibucket";
        String keyName            = "samples/small1.gz";
        String filePath           = "/home/mark/data/sample_n1.gz";  
        
        if (args.length == 0) {

        }
        else if (args.length == 1){
        	
        	filePath = args[0];
        }
        else if (args.length == 2){
        	filePath = args[0];
        	keyName = args[1];
        }
        
        // Specifying Signature Version in Request Authentication
        System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true");
        System.out.println("SDKGlobalConfiguration Changed!");
               
        
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        
        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region cnNorth1 = Region.getRegion(Regions.CN_NORTH_1);
        s3.setRegion(cnNorth1);
     
        TransferManager tm = new TransferManager(s3);   
        System.out.println("Hello");
        // TransferManager processes all transfers asynchronously, 
        // so this call will return immediately.
        Upload upload = tm.upload(
        		existingBucketName, keyName, new File(filePath));
        System.out.println("Hello2");
        
        try {
        	// Or you can block and wait for the upload to finish
        	upload.waitForCompletion();
        	System.out.println("Upload complete.");
        	tm.shutdownNow();
        } catch (AmazonClientException amazonClientException) {
        	System.out.println("Unable to upload file, upload was aborted.");
        	amazonClientException.printStackTrace();
        }
    }
}