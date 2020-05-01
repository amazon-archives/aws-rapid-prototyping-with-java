package com.amazon.aws.prototyping;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

abstract class AbstractFunction {
    protected APIGatewayProxyResponseEvent ok() {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(Integer.valueOf(200));
        response.setBody("ok");
        return response;
    }
}
