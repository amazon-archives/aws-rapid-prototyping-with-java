package com.amazon.aws.prototyping.apigateway;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonFunction extends AbstractFunction {

    public APIGatewayProxyResponseEvent parse(APIGatewayProxyRequestEvent event, Context context) {
        String json = "{\"name\": \"example\", \"countries\": [\"japan\", \"usa\"]}";
        try {
            Map<String, Object> result = new ObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {
            });

            System.out.println(result.get("name")); // example

            List<Object> countries = (List<Object>) result.get("countries");
            System.out.println(countries.get(0)); // japan
            System.out.println(countries.get(1)); // usa
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return ok();
    }

    public APIGatewayProxyResponseEvent serialize(APIGatewayProxyRequestEvent event, Context context) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "example");
        map.put("contries", Arrays.asList("japan", "usa"));

        try {
            String json = new ObjectMapper().writeValueAsString(map);
            System.out.println(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return ok();
    }
}
