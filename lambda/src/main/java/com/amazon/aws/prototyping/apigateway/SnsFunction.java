package com.amazon.aws.prototyping.apigateway;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;

public class SnsFunction extends AbstractFunction {

    private static final String TOPIC_ARN = System.getenv("TOPIC_ARN");

    private static final AmazonSNS SNS = AmazonSNSClientBuilder.defaultClient();

    public APIGatewayProxyResponseEvent publish(APIGatewayProxyRequestEvent event, Context context) {
        SNS.publish(TOPIC_ARN, event.getBody());

        return ok();
    }
}
