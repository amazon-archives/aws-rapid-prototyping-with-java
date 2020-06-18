package com.amazon.aws.prototyping.apigateway;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class SqsFunction extends AbstractFunction {

    private static final String QUEUE_URL = System.getenv("QUEUE_URL");

    private static final AmazonSQS SQS = AmazonSQSClientBuilder.defaultClient();

    public APIGatewayProxyResponseEvent send(APIGatewayProxyRequestEvent event, Context context) {
        SQS.sendMessage(QUEUE_URL, event.getBody());
        return ok();
    }
}
