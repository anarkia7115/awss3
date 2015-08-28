package com.gcbi.hadoop.awss3;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;

public class S3List {

	String bucketName = "gcbibucket";
	AmazonS3 s3;
	
	public S3List() {
		// TODO Auto-generated constructor stub
	
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
        
        s3 = new AmazonS3Client(credentials);
        Region cnNorth1 = Region.getRegion(Regions.CN_NORTH_1);
        s3.setRegion(cnNorth1);
	}
	
	public static void main(String[] args) {

		S3List sl = new S3List();
		System.out.println("Hello1");
		sl.listObjects();
		System.out.println("Hello2");

	}
	
	public void listObjects() {

		S3List s3l = new S3List();
		
		ObjectListing ol = s3l.s3.listObjects(bucketName);
		
		List<String> commonPrefixes = ol.getCommonPrefixes();
		
		String checkBucketName = ol.getBucketName();

		for (String prefix : commonPrefixes) {
			System.out.println(prefix);
		}
		
		System.out.println(checkBucketName);
	}
}
