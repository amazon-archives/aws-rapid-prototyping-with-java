package com.amazon.aws.prototyping;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

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

public class JdbcFunction extends AbstractFunction {
    private static final String DATABASE_ENDPOINT = System.getenv("DATABASE_ENDPOINT");
    private static final String SECRET_ARN = System.getenv("SECRET_ARN");
    private static final String DATABASE_NAME = System.getenv("DATABASE_NAME");

    private static final AWSSecretsManager SECRETS_MANAGER = AWSSecretsManagerClientBuilder.defaultClient();

    public JdbcFunction() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public APIGatewayProxyResponseEvent createTable(APIGatewayProxyRequestEvent event, Context context) {
        Pair<String, String> idpass = getUsernameAndPassword();

        String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s?user=%s&password=%s&useSSL=false", DATABASE_ENDPOINT,
                DATABASE_NAME, idpass.getLeft(), idpass.getRight());
        try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
            System.out.println("creating table...");
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE user (id INT, name TEXT)");
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO user VALUES (1, 'user1'), (2, 'user2')");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return ok();
    }
    
    public APIGatewayProxyResponseEvent select(APIGatewayProxyRequestEvent event, Context context) {
        Pair<String, String> idpass = getUsernameAndPassword();

        String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s?user=%s&password=%s&useSSL=false", DATABASE_ENDPOINT,
                DATABASE_NAME, idpass.getLeft(), idpass.getRight());
        try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM user")) {
                while (resultSet.next()) {
                    System.out.printf("id=%s, name=%s\n", resultSet.getInt("id"), resultSet.getString("name"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return ok();
    }

    public Pair<String, String> getUsernameAndPassword() {

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
