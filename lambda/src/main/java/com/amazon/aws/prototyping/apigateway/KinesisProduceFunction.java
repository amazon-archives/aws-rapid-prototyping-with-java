package com.amazon.aws.prototyping.apigateway;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.RandomStringUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

public class KinesisProduceFunction extends AbstractFunction {

    private static final String KINESIS_STREAM_NAME = System.getenv("KINESIS_STREAM_NAME");

    private static final AmazonKinesis KINESIS = AmazonKinesisClientBuilder.defaultClient();

    public APIGatewayProxyResponseEvent putRecord(APIGatewayProxyRequestEvent event, Context context) {

        KINESIS.putRecord(KINESIS_STREAM_NAME, ByteBuffer.wrap(event.getBody().getBytes()),
                RandomStringUtils.randomAlphanumeric(10));

        return ok();
    }
}
