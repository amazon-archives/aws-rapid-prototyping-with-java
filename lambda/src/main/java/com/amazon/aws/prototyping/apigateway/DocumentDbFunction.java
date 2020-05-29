package com.amazon.aws.prototyping.apigateway;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class DocumentDbFunction extends AbstractFunction {

    private static final String SECRET_ARN = System.getenv("SECRET_ARN");
    private static final String CLUSTER_ENDPOINT = System.getenv("CLUSTER_ENDPOINT");
    private static final String DATABASE_NAME = "test";

    private static final AWSSecretsManager SECRETS_MANAGER = AWSSecretsManagerClientBuilder.defaultClient();

    public APIGatewayProxyResponseEvent insert(APIGatewayProxyRequestEvent event, Context context) {
        Pair<String, String> idpass = getUsernameAndPassword();

        try (MongoClient client = new MongoClient(new MongoClientURI(
                String.format("mongodb://%s:%s@%s/%s?replicaSet=rs0&readpreference=secondaryPreferred",
                        idpass.getLeft(), idpass.getRight(), CLUSTER_ENDPOINT, DATABASE_NAME)))) {

            MongoDatabase db = client.getDatabase(DATABASE_NAME);

            MongoCollection<Document> userCollection = db.getCollection("user");
            Document document = Document.parse(event.getBody());
            userCollection.insertOne(document);

            return ok();
        }
    }

    public APIGatewayProxyResponseEvent delete(APIGatewayProxyRequestEvent event, Context context) {
        Pair<String, String> idpass = getUsernameAndPassword();

        try (MongoClient client = new MongoClient(new MongoClientURI(
                String.format("mongodb://%s:%s@%s/%s?replicaSet=rs0&readpreference=secondaryPreferred",
                        idpass.getLeft(), idpass.getRight(), CLUSTER_ENDPOINT, DATABASE_NAME)))) {

            MongoDatabase db = client.getDatabase(DATABASE_NAME);
            db.drop();

            return ok();
        }
    }

    public APIGatewayProxyResponseEvent find(APIGatewayProxyRequestEvent event, Context context) {
        Pair<String, String> idpass = getUsernameAndPassword();

        List<Document> result = new ArrayList<>();
        try (MongoClient client = new MongoClient(new MongoClientURI(
                String.format("mongodb://%s:%s@%s/%s?replicaSet=rs0&readpreference=secondaryPreferred",
                        idpass.getLeft(), idpass.getRight(), CLUSTER_ENDPOINT, DATABASE_NAME)))) {

            MongoDatabase db = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> userCollection = db.getCollection("user");

            Map<String, String> queryStringParameters = event.getQueryStringParameters();
            if (MapUtils.isEmpty(queryStringParameters)) {
                for (Document document : userCollection.find()) {
                    result.add(document);
                }
            } else {
                BasicDBObject queryObject = new BasicDBObject();
                for (Map.Entry<String, String> queryParameter : queryStringParameters.entrySet()) {
                    String value = queryParameter.getValue();

                    Object queryValue;
                    if (NumberUtils.isCreatable(value)) {
                        queryValue = Integer.valueOf(value);
                    } else {
                        queryValue = value;
                    }
                    queryObject.put(queryParameter.getKey(), queryValue);
                }
                for (Document document : userCollection.find(queryObject)) {
                    result.add(document);
                }
            }

            APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
            response.setStatusCode(Integer.valueOf(200));
            response.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
            response.setBody(new ObjectMapper().writeValueAsString(result));
            return response;

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private Pair<String, String> getUsernameAndPassword() {

        GetSecretValueResult getSecretValueResult = SECRETS_MANAGER
                .getSecretValue(new GetSecretValueRequest().withSecretId(SECRET_ARN));
        try {
            Map<String, String> secretMap = new ObjectMapper().readValue(getSecretValueResult.getSecretString(),
                    new TypeReference<Map<String, String>>() {
                    });
            return Pair.of(secretMap.get("username"), secretMap.get("password"));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
