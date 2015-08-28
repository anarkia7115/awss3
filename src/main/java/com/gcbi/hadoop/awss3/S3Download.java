package com.gcbi.hadoop.awss3;

import java.io.File;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;

public class S3Download {

	public static void main(String[] args) {

		String bucketName = "gcbiupload";

		String key = "sample_n1.gz";
		
		String destPath = "/home/mark/testS3Downloads/" + key;

		// Credential
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (~/.aws/credentials), and is in valid format.",
					e);
		}
		AmazonS3Client s3 = new AmazonS3Client(credentials);

		// s3
		s3.setRegion(Region.getRegion(Regions.CN_NORTH_1));

		// transfer
		TransferManager trans = new TransferManager(s3);

		// s3 object
		S3ObjectId s3ObjId = new S3ObjectId(bucketName, key);

		// GetObjectRequest
		GetObjectRequest getObjReq = new GetObjectRequest(s3ObjId);

		// Listen
		ProgressListener progLis = new ProgressListener();

		// download
		
		File destFile = new File(destPath);
		destFile.setWritable(true);
		Download dl = trans.download(getObjReq, destFile, progLis);

		try {
			dl.waitForCompletion();
		} catch (AmazonServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AmazonClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Transferred: "
				+ dl.getProgress().getBytesTransferred());
		System.out.println("Percentage: "
				+ dl.getProgress().getPercentTransferred());

		try {
			
			// test if the Download Job is Done.
			if (dl.isDone()) {
				System.out.println("Download Complete!");
				dl.abort();
			} else {
				System.out
						.println("Download is NOT Finished, Abort with Error");
				dl.abort();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class ProgressListener implements S3ProgressListener {

	public void progressChanged(ProgressEvent progressEvent) {
		System.out.println(
				"getBytesTransferred: " 
				+ progressEvent.getBytesTransferred());

	}

	public void onPersistableTransfer(PersistableTransfer arg0) {
	}
};
