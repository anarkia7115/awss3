package com.gcbi.hadoop.awsemr;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;


public class EmrListSteps {

	public static void main(String[] args) {
		
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
        
        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);

        Region cnNorth1 = Region.getRegion(Regions.CN_NORTH_1);
        emr.setRegion(cnNorth1);
        
//        ListClustersRequest listClustersRequest = new ListClustersRequest();
//        emr.listClusters();
        
        
        
//        emr.listClusters();
        ListStepsRequest listStepRequest = new ListStepsRequest();
        String cid = "j-2LCS9P4SUSFZN";
        listStepRequest.setClusterId(cid);
//        //ListStepsResult stepResult = emr.listSteps(lsr);
        
        ListStepsResult lsr = emr.listSteps(listStepRequest);
        
        lsr.getSteps();
        System.out.println("End");
	}
}
