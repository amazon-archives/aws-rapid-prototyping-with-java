package com.amazon.aws.prototyping.apigateway;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

public class HttpFunction extends AbstractFunction {

    private static final HttpClient HTTP_CLIENT = HttpClients.createDefault();

    public APIGatewayProxyResponseEvent sendGet(APIGatewayProxyRequestEvent event, Context context) {
        HttpGet get = new HttpGet("http://" + event.getQueryStringParameters().get("host"));
        try {
            HttpResponse response = HTTP_CLIENT.execute(get);
            outputHttpResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ok();
    }

    public APIGatewayProxyResponseEvent sendPost(APIGatewayProxyRequestEvent event, Context context) {
        HttpPost post = new HttpPost("http://" + event.getQueryStringParameters().get("host"));
        post.setEntity(new StringEntity("{'message': 'hello'}", ContentType.APPLICATION_JSON));

        try {
            HttpResponse response = HTTP_CLIENT.execute(post);
            outputHttpResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ok();
    }

    private void outputHttpResponse(HttpResponse response) throws IOException {
        try (InputStream is = response.getEntity().getContent()) {
            for (String line : IOUtils.readLines(is, "UTF-8")) {
                System.out.println(line);
            }
        }
    }
}
