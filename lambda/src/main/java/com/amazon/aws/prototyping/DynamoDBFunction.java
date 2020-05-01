package com.amazon.aws.prototyping;

import java.util.Arrays;
import java.util.HashMap;

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

public class DynamoDBFunction extends AbstractFunction {

    // https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Java.html

    private static final String TABLE_NAME = System.getenv("TABLE_NAME");
    private static final String INDED_NAME = System.getenv("INDEX_NAME");

    private static final com.amazonaws.services.dynamodbv2.document.DynamoDB DYNAMODB = new com.amazonaws.services.dynamodbv2.document.DynamoDB(
            AmazonDynamoDBClientBuilder.defaultClient());

    public APIGatewayProxyResponseEvent get(APIGatewayProxyRequestEvent event, Context context) {
        {
            System.out.println("--- Scan ---");
            ItemCollection<ScanOutcome> items = DYNAMODB.getTable(TABLE_NAME)
                    .scan(new ScanSpec().withMaxResultSize(10));
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
            ItemCollection<QueryOutcome> items = DYNAMODB.getTable(TABLE_NAME).getIndex(INDED_NAME)
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
        HashMap<String, Object> info = new HashMap<String, Object>();
        info.put("rating", Double.valueOf(5.8));
        info.put("actors", Arrays.asList("Willem Dafoe", "Robert Pattinson", "Valeriia Karaman"));

        DYNAMODB.getTable(TABLE_NAME)
                .putItem(new Item().withPrimaryKey("year", 2019, "title", "The Lighthouse").withMap("info", info));

        return ok();
    }
}
