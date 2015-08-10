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
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;


public class S3UploadDir {

    public static void main(String[] args) throws Exception {
        String existingBucketName = "gcbibucket";
        String keyName            = "samples/small1.gz";
        //File filePath           = "/home/mark/data/sample_n1.gz";  
        //final long startTime = System.currentTimeMillis() / 1000;

       	File filePath = new File(args[0]);
        keyName = args[1];
        
        File[] listOfFiles = filePath.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                System.out.println(file.getName());
            }
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

        
        // You can ask the upload for its progress, or you can 
        // add a ProgressListener to your request to receive notifications 
        // when bytes are transferred.
        /*
         *  uploadDirectory(
         *  
         *  java.lang.String bucketName, 
         *  java.lang.String virtualDirectoryKeyPrefix, 
         *  java.io.File directory, 
         *  boolean includeSubdirectories
         *  
         *  )
         * */
        
        boolean includeSubdirectories = true;
        
        MultipleFileUpload upload = tm.uploadDirectory(existingBucketName, keyName, filePath, includeSubdirectories);
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