package com.gcbi.hadoop.awss3;

import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class S3UploadTrack {

    public static void main(String[] args) throws Exception {
        String existingBucketName = "gcbiupload";
        String keyName            = "sample_n1.gz";
        String filePath           = "/home/mark/testS3Downloads/" + keyName;  
        
		// credencial
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
        AmazonS3Client s3 = new AmazonS3Client(credentials);
    
        // s3
        s3.setRegion(Region.getRegion(Regions.CN_NORTH_1));

        // transfer
        TransferManager tm = new TransferManager(s3);
        
        // For more advanced uploads, you can create a request object 
        // and supply additional request parameters (ex: progress listeners,
        // canned ACLs, etc.)
        PutObjectRequest request = new PutObjectRequest(
        		existingBucketName, keyName, new File(filePath));
        
        // You can ask the upload for its progress, or you can 
        // add a ProgressListener to your request to receive notifications 
        // when bytes are transferred.
        request.setGeneralProgressListener(new ProgressListener() {
			public void progressChanged(ProgressEvent progressEvent) {
				System.out.println("Transferred bytes: " + 
						progressEvent.getBytesTransferred()
						);
			}
		});

        // TransferManager processes all transfers asynchronously, 
        // so this call will return immediately.
        Upload upload = tm.upload(request);
        
        try {
        	// You can block and wait for the upload to finish
        	upload.waitForCompletion();
        } catch (AmazonClientException amazonClientException) {
        	System.out.println("Unable to upload file, upload aborted.");
        	amazonClientException.printStackTrace();
        }
        
        System.out.println("Percentage: "
        		+ upload.getProgress().getPercentTransferred()
        		);
        
        if (upload.isDone()){
        	
        	upload.abort();
        	System.out.println("Upload Complete");
        }
        else {
        	
        	upload.abort();
        	System.out.println("Upload Ended with Errors");
        }
    }
}