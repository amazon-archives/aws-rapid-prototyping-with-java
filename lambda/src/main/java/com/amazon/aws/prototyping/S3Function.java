package com.amazon.aws.prototyping;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringInputStream;

public class S3Function extends AbstractFunction {

    private static final AmazonS3 S3 = AmazonS3ClientBuilder.defaultClient();
    private static final String BUCKET_NAME = System.getenv("BUCKET_NAME");

    public APIGatewayProxyResponseEvent putObject(APIGatewayProxyRequestEvent event, Context context) {
        String content = "hello";

        // String
        S3.putObject(BUCKET_NAME, "sample-string.txt", content);

        // InputStream
        try (InputStream is = new StringInputStream(content)) {
            S3.putObject(BUCKET_NAME, "sample-inputstream.txt", is, new ObjectMetadata());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ok();
    }

    public APIGatewayProxyResponseEvent getObject(APIGatewayProxyRequestEvent event, Context context) {
        S3Object s3Object = S3.getObject(BUCKET_NAME, "sample-string.txt");

        try (InputStream is = s3Object.getObjectContent()) {
            String content = IOUtils.toString(is, "UTF-8");
            System.out.println("content: " + content);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ok();
    }
}
