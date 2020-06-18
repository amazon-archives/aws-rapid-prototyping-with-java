package com.amazon.aws.prototyping.apigateway;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DynamoDBFunction extends AbstractFunction {

    // https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Java.html

    private static final String TABLE_NAME = System.getenv("TABLE_NAME");
    private static final String INDEX_NAME = System.getenv("INDEX_NAME");

    private static final com.amazonaws.services.dynamodbv2.document.DynamoDB DYNAMODB =
            new com.amazonaws.services.dynamodbv2.document.DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());

    public APIGatewayProxyResponseEvent get(APIGatewayProxyRequestEvent event, Context context) {
        {
            System.out.println("--- Scan ---");
            ItemCollection<ScanOutcome> items =
                    DYNAMODB.getTable(TABLE_NAME).scan(new ScanSpec().withMaxResultSize(10));
            for (Item item : items) {
                System.out.println(item);
            }
        }
        {
            System.out.println("--- GetItem ---");
            Item item = DYNAMODB.getTable(TABLE_NAME)
                    .getItem(new GetItemSpec().withPrimaryKey("year", 1933, "title", "King Kong"));
            System.out.println(item);
        }
        {
            System.out.println("--- Query ---");
            ItemCollection<QueryOutcome> items = DYNAMODB.getTable(TABLE_NAME)
                    .query(new QuerySpec().withKeyConditionExpression("#yr = :yyyy")
                            .withNameMap(new NameMap().with("#yr", "year"))
                            .withValueMap(new ValueMap().withNumber(":yyyy", 2000)).withMaxResultSize(10));
            for (Item item : items) {
                System.out.println(item);
            }
        }
        {
            System.out.println("--- Query by Index ---");
            ItemCollection<QueryOutcome> items = DYNAMODB.getTable(TABLE_NAME).getIndex(INDEX_NAME)
                    .query(new QuerySpec().withKeyConditionExpression("#ttl = :title")
                            .withNameMap(new NameMap().with("#ttl", "title"))
                            .withValueMap(new ValueMap().withString(":title", "Spider-Man")));
            for (Item item : items) {
                System.out.println(item);
            }
        }

        return ok();
    }

    public APIGatewayProxyResponseEvent put(APIGatewayProxyRequestEvent event, Context context) {

        Movie movie;
        try {
            movie = new ObjectMapper().readValue(event.getBody(), Movie.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        HashMap<String, Object> info = new HashMap<>();
        info.put("rating", RandomUtils.nextDouble(0, 10));
        info.put("actors", Collections.singletonList(RandomStringUtils.randomAlphanumeric(10)));
        movie.setInfo(info);

        DYNAMODB.getTable(TABLE_NAME).putItem(new Item()
                .withPrimaryKey("year", movie.getYear(), "title", movie.getTitle()).withMap("info", movie.getInfo()));

        return ok();
    }

    public static class Movie {
        private int year;

        public void setYear(int year) {
            this.year = year;
        }

        public int getYear() {
            return year;
        }

        private String title;

        public void setTitle(String title) {
            this.title = title;
        }

        public String getTitle() {
            return title;
        }

        private Map<String, Object> info;

        public void setInfo(Map<String, Object> info) {
            this.info = info;
        }

        public Map<String, Object> getInfo() {
            return info;
        }
    }
}
