package com.amazon.aws.prototyping.apigateway;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;

public class EC2Function extends AbstractFunction {

    private static final AmazonEC2 EC2 = AmazonEC2ClientBuilder.defaultClient();

    private static final String INSTANCE_ID = System.getenv("INSTANCE_ID");

    public APIGatewayProxyResponseEvent startAndWait(APIGatewayProxyRequestEvent event, Context context) {
        EC2.startInstances(new StartInstancesRequest().withInstanceIds(INSTANCE_ID));

        Waiter<DescribeInstancesRequest> waiter = EC2.waiters().instanceRunning();
        System.out.println("waiting for starting ...");
        waiter.run(new WaiterParameters<>(new DescribeInstancesRequest().withInstanceIds(INSTANCE_ID)));
        System.out.println("done");

        // API Gateway may response 504 Gateway Timeout because of Integration Timeout
        return ok();
    }

    public APIGatewayProxyResponseEvent stopAndWait(APIGatewayProxyRequestEvent event, Context context) {
        EC2.stopInstances(new StopInstancesRequest().withInstanceIds(INSTANCE_ID));

        Waiter<DescribeInstancesRequest> waiter = EC2.waiters().instanceStopped();
        System.out.println("waiting for stoping ...");
        waiter.run(new WaiterParameters<>(new DescribeInstancesRequest().withInstanceIds(INSTANCE_ID)));
        System.out.println("done");

        // API Gateway may response 504 Gateway Timeout because of Integration Timeout
        return ok();
    }
}
