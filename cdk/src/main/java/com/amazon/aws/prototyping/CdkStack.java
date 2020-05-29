package com.amazon.aws.prototyping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Transformer;

import com.amazon.aws.prototyping.apigateway.DocumentDbFunction;
import com.amazon.aws.prototyping.apigateway.DynamoDBFunction;
import com.amazon.aws.prototyping.apigateway.EC2Function;
import com.amazon.aws.prototyping.apigateway.HttpFunction;
import com.amazon.aws.prototyping.apigateway.JdbcFunction;
import com.amazon.aws.prototyping.apigateway.JsonFunction;
import com.amazon.aws.prototyping.apigateway.KinesisProduceFunction;
import com.amazon.aws.prototyping.apigateway.S3Function;
import com.amazon.aws.prototyping.apigateway.SnsFunction;
import com.amazon.aws.prototyping.apigateway.SqsFunction;
import com.amazon.aws.prototyping.eventsource.DynamoDBStreamFunction;
import com.amazon.aws.prototyping.eventsource.KinesisConsumeFunction;
import com.amazon.aws.prototyping.eventsource.SnsSubscribedFunction;
import com.amazon.aws.prototyping.eventsource.SqsReceiveFunction;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.apigateway.LambdaIntegration;
import software.amazon.awscdk.services.apigateway.MethodLoggingLevel;
import software.amazon.awscdk.services.apigateway.Resource;
import software.amazon.awscdk.services.apigateway.RestApi;
import software.amazon.awscdk.services.apigateway.StageOptions;
import software.amazon.awscdk.services.docdb.CfnDBCluster;
import software.amazon.awscdk.services.docdb.CfnDBClusterParameterGroup;
import software.amazon.awscdk.services.docdb.CfnDBInstance;
import software.amazon.awscdk.services.docdb.CfnDBSubnetGroup;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.GlobalSecondaryIndexProps;
import software.amazon.awscdk.services.dynamodb.StreamViewType;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.ec2.AmazonLinuxGeneration;
import software.amazon.awscdk.services.ec2.AmazonLinuxImage;
import software.amazon.awscdk.services.ec2.ISubnet;
import software.amazon.awscdk.services.ec2.Instance;
import software.amazon.awscdk.services.ec2.InstanceClass;
import software.amazon.awscdk.services.ec2.InstanceSize;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.Peer;
import software.amazon.awscdk.services.ec2.Port;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.SubnetConfiguration;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.SubnetType;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.kinesis.Stream;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.StartingPosition;
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource;
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SnsEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.rds.DatabaseInstance;
import software.amazon.awscdk.services.rds.DatabaseInstanceEngine;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.secretsmanager.Secret;
import software.amazon.awscdk.services.secretsmanager.SecretStringGenerator;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;

public class CdkStack extends Stack {
    public CdkStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public CdkStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        StageOptions deployOptions = StageOptions.builder().loggingLevel(MethodLoggingLevel.INFO).dataTraceEnabled(true)
                .build();
        RestApi api = RestApi.Builder.create(this, "JavaSamplesRestApi").deployOptions(deployOptions).build();

        List<SubnetConfiguration> subnetConfigurations = Arrays.asList(
                SubnetConfiguration.builder().cidrMask(24).name("public").subnetType(SubnetType.PUBLIC).build(),
                SubnetConfiguration.builder().cidrMask(24).name("private").subnetType(SubnetType.PRIVATE).build());
        Vpc vpc = Vpc.Builder.create(this, "SampleVpc").subnetConfiguration(subnetConfigurations).maxAzs(2).build();

        createHttpSample(api);
        createS3Sample(api);
        createDynamoDbSample(api);
        createEC2Sample(api, vpc);
        createJsonSample(api);
        createRdbSample(api, vpc);
        createDocumentDbSample(api, vpc);
        createKinesisSample(api);
        createSqlSample(api);
        createSnsSample(api);
    }

    private void createHttpSample(RestApi api) {
        Resource httpResource = api.getRoot().addResource("http");

        Function sendGetfunction = createFunction(HttpFunction.class, "sendGet");
        Function sendPostFunction = createFunction(HttpFunction.class, "sendPost");

        httpResource.addMethod("GET", LambdaIntegration.Builder.create(sendGetfunction).proxy(true).build());
        httpResource.addMethod("POST", LambdaIntegration.Builder.create(sendPostFunction).proxy(true).build());
    }

    private void createS3Sample(RestApi api) {
        Bucket bucket = Bucket.Builder.create(this, "SampleBucket").build();

        Function getObjectFunction = createFunction(S3Function.class, "getObject");
        getObjectFunction.addEnvironment("BUCKET_NAME", bucket.getBucketName());
        bucket.grantRead(getObjectFunction);

        Function putObjectFunction = createFunction(S3Function.class, "putObject");
        putObjectFunction.addEnvironment("BUCKET_NAME", bucket.getBucketName());
        bucket.grantReadWrite(putObjectFunction);

        Resource s3Resource = api.getRoot().addResource("s3");
        s3Resource.addMethod("GET", LambdaIntegration.Builder.create(getObjectFunction).proxy(true).build());
        s3Resource.addMethod("PUT", LambdaIntegration.Builder.create(putObjectFunction).proxy(true).build());
    }

    private void createDynamoDbSample(RestApi api) {
        Attribute attributeYear = Attribute.builder().name("year").type(AttributeType.NUMBER).build();
        Attribute attributeTitle = Attribute.builder().name("title").type(AttributeType.STRING).build();
        Table table = Table.Builder.create(this, "SampleTable").partitionKey(attributeYear).sortKey(attributeTitle)
                .stream(StreamViewType.NEW_AND_OLD_IMAGES).build();
        GlobalSecondaryIndexProps index = GlobalSecondaryIndexProps.builder().indexName("title-year-index")
                .partitionKey(attributeTitle).sortKey(attributeYear).build();
        table.addGlobalSecondaryIndex(index);

        Function getFunction = createFunction(DynamoDBFunction.class, "get");
        getFunction.addEnvironment("TABLE_NAME", table.getTableName());
        getFunction.addEnvironment("INDEX_NAME", index.getIndexName());
        table.grantReadData(getFunction);

        Function putFunction = createFunction(DynamoDBFunction.class, "put");
        putFunction.addEnvironment("TABLE_NAME", table.getTableName());
        table.grantReadWriteData(putFunction);

        Resource dynamodbResource = api.getRoot().addResource("dynamodb");
        dynamodbResource.addMethod("GET", LambdaIntegration.Builder.create(getFunction).proxy(true).build());
        dynamodbResource.addMethod("PUT", LambdaIntegration.Builder.create(putFunction).proxy(true).build());

        Function streamFunction = createFunction(DynamoDBStreamFunction.class, "handle");
        DynamoEventSource eventSource = DynamoEventSource.Builder.create(table)
                .startingPosition(StartingPosition.LATEST).build();
        streamFunction.addEventSource(eventSource);
    }

    private void createEC2Sample(RestApi api, Vpc vpc) {
        Instance instance = Instance.Builder.create(this, "SampleInstance")
                .machineImage(
                        AmazonLinuxImage.Builder.create().generation(AmazonLinuxGeneration.AMAZON_LINUX_2).build())
                .instanceType(InstanceType.of(InstanceClass.BURSTABLE2, InstanceSize.MICRO)).vpc(vpc).build();

        PolicyStatement describeInstancesPolicy = PolicyStatement.Builder.create().effect(Effect.ALLOW)
                .actions(Arrays.asList("ec2:describeInstances")).resources(Arrays.asList("*")).build();

        Function startFunction = createFunction(EC2Function.class, "startAndWait", Duration.minutes(3), null);
        startFunction.addEnvironment("INSTANCE_ID", instance.getInstanceId());
        startFunction.addToRolePolicy(PolicyStatement.Builder.create().effect(Effect.ALLOW)
                .actions(Arrays.asList("ec2:startInstances")).resources(Arrays.asList(String
                        .format("arn:aws:ec2:%s:%s:instance/%s", getRegion(), getAccount(), instance.getInstanceId())))
                .build());
        startFunction.addToRolePolicy(describeInstancesPolicy);

        Function stopFunction = createFunction(EC2Function.class, "stopAndWait", Duration.minutes(3), null);
        stopFunction.addEnvironment("INSTANCE_ID", instance.getInstanceId());
        stopFunction.addToRolePolicy(PolicyStatement.Builder.create().effect(Effect.ALLOW)
                .actions(Arrays.asList("ec2:stopInstances")).resources(Arrays.asList(String
                        .format("arn:aws:ec2:%s:%s:instance/%s", getRegion(), getAccount(), instance.getInstanceId())))
                .build());
        stopFunction.addToRolePolicy(describeInstancesPolicy);

        Resource ec2Resource = api.getRoot().addResource("ec2");
        ec2Resource.addResource("start").addMethod("POST",
                LambdaIntegration.Builder.create(startFunction).proxy(true).build());
        ec2Resource.addResource("stop").addMethod("POST",
                LambdaIntegration.Builder.create(stopFunction).proxy(true).build());
    }

    private void createJsonSample(RestApi api) {
        Function parseFunction = createFunction(JsonFunction.class, "parse");
        Function serializeFunction = createFunction(JsonFunction.class, "serialize");

        Resource jsonResource = api.getRoot().addResource("json");
        jsonResource.addResource("parse").addMethod("POST",
                LambdaIntegration.Builder.create(parseFunction).proxy(true).build());
        jsonResource.addResource("serialize").addMethod("POST",
                LambdaIntegration.Builder.create(serializeFunction).proxy(true).build());
    }

    private void createRdbSample(RestApi api, Vpc vpc) {
        Secret secret = Secret.Builder.create(this, "RdsSampleUserPassword")
                .generateSecretString(
                        SecretStringGenerator.builder().secretStringTemplate(String.format("{\"username\": \"test\"}"))
                                .generateStringKey("password").excludePunctuation(true).build())
                .build();

        String databaseName = "sample";
        SecurityGroup rdsSecurityGroup = SecurityGroup.Builder.create(this, "SampleRdsSecurityGroup").vpc(vpc).build();
        rdsSecurityGroup.addIngressRule(Peer.ipv4(vpc.getVpcCidrBlock()), Port.tcp(3306));

        DatabaseInstance instance = DatabaseInstance.Builder.create(this, "SampleRds")
                .engine(DatabaseInstanceEngine.MYSQL).engineVersion("5.7.28")
                .instanceClass(InstanceType.of(InstanceClass.BURSTABLE2, InstanceSize.MICRO)).vpc(vpc)
                .vpcPlacement(SubnetSelection.builder().subnetType(SubnetType.PRIVATE).build())
                .securityGroups(Arrays.asList(rdsSecurityGroup))
                .masterUsername(secret.secretValueFromJson("username").toString())
                .masterUserPassword(secret.secretValueFromJson("password")).databaseName(databaseName).build();

        Function createTableFunction = createFunction(JdbcFunction.class, "createTable", Duration.seconds(30), vpc);
        Function selectFunction = createFunction(JdbcFunction.class, "select", Duration.seconds(30), vpc);

        for (Function function : Arrays.asList(createTableFunction, selectFunction)) {
            function.addEnvironment("DATABASE_ENDPOINT", instance.getDbInstanceEndpointAddress());
            function.addEnvironment("DATABASE_NAME", databaseName);
            function.addEnvironment("SECRET_ARN", secret.getSecretArn());
            secret.grantRead(function);
        }

        Resource jdbcResource = api.getRoot().addResource("jdbc");
        jdbcResource.addResource("createTable").addMethod("POST",
                LambdaIntegration.Builder.create(createTableFunction).proxy(true).build());
        jdbcResource.addResource("select").addMethod("GET",
                LambdaIntegration.Builder.create(selectFunction).proxy(true).build());
    }

    private void createDocumentDbSample(RestApi api, Vpc vpc) {
        Secret secret = Secret.Builder.create(this, "DocumentDbSampleUserPassword")
                .generateSecretString(
                        SecretStringGenerator.builder().secretStringTemplate(String.format("{\"username\": \"test\"}"))
                                .generateStringKey("password").excludePunctuation(true).build())
                .build();

        SecurityGroup securityGroup = SecurityGroup.Builder.create(this, "SampleDocumentDBSecurityGroup").vpc(vpc)
                .build();
        securityGroup.addIngressRule(Peer.ipv4(vpc.getVpcCidrBlock()), Port.tcp(27017));

        Map<String, String> parameterMap = new HashMap<>();
        parameterMap.put("tls", "disabled");
        CfnDBClusterParameterGroup parameterGroup = CfnDBClusterParameterGroup.Builder
                .create(this, "SampleDocumentDBParameterGroup").name("sample-document-db-parameter-group")
                .description("DBParameterGroup for SampleDocumentDBCluster").family("docdb3.6").parameters(parameterMap)
                .build();

        List<String> subnetIds = new ArrayList<>(
                CollectionUtils.collect(vpc.getPrivateSubnets(), new Transformer<ISubnet, String>() {
                    @Override
                    public String transform(ISubnet subnet) {
                        return subnet.getSubnetId();
                    }
                }));
        // both db subnet group name and cluster name need to be lower case, because it
        // is changed to lower case by cdk for some reason.
        CfnDBSubnetGroup subnetGroup = CfnDBSubnetGroup.Builder.create(this, "SampleDocumentDBSubnetGroup")
                .subnetIds(subnetIds).dbSubnetGroupName("sample-document-db-subnet-group")
                .dbSubnetGroupDescription("DBSubnetGroup for SampleDocumentDBCluster").build();

        CfnDBCluster cluster = CfnDBCluster.Builder.create(this, "SampleDocumentDBCluster")
                .dbClusterIdentifier("sample-document-db-cluster").dbClusterParameterGroupName(parameterGroup.getName())
                .dbSubnetGroupName(subnetGroup.getDbSubnetGroupName())
                .vpcSecurityGroupIds(Arrays.asList(securityGroup.getSecurityGroupId()))
                .masterUsername(secret.secretValueFromJson("username").toString())
                .masterUserPassword(secret.secretValueFromJson("password").toString()).build();
        cluster.addDependsOn(subnetGroup);

        CfnDBInstance instance = CfnDBInstance.Builder.create(this, "SampleDocumentDB")
                .dbClusterIdentifier(cluster.getDbClusterIdentifier()).dbInstanceClass("db.r5.large").build();
        instance.addDependsOn(cluster);

        Function insertFunction = createFunction(DocumentDbFunction.class, "insert", Duration.seconds(30), vpc);
        Function deleteFunction = createFunction(DocumentDbFunction.class, "delete", Duration.seconds(30), vpc);
        Function findFunction = createFunction(DocumentDbFunction.class, "find", Duration.seconds(30), vpc);

        for (Function function : Arrays.asList(insertFunction, deleteFunction, findFunction)) {
            function.addEnvironment("CLUSTER_ENDPOINT", cluster.getAttrEndpoint());
            function.addEnvironment("SECRET_ARN", secret.getSecretArn());
            secret.grantRead(function);
        }

        Resource documentdbResource = api.getRoot().addResource("documentdb");
        documentdbResource.addResource("insert").addMethod("POST",
                LambdaIntegration.Builder.create(insertFunction).proxy(true).build());
        documentdbResource.addResource("delete").addMethod("POST",
                LambdaIntegration.Builder.create(deleteFunction).proxy(true).build());
        documentdbResource.addResource("find").addMethod("GET",
                LambdaIntegration.Builder.create(findFunction).proxy(true).build());
    }

    private void createKinesisSample(RestApi api) {
        Stream stream = Stream.Builder.create(this, "SampleKinesisStream").build();

        Function putRecordFunction = createFunction(KinesisProduceFunction.class, "putRecord");
        putRecordFunction.addEnvironment("KINESIS_STREAM_NAME", stream.getStreamName());
        stream.grantWrite(putRecordFunction);

        api.getRoot().addResource("kinesis").addMethod("PUT",
                LambdaIntegration.Builder.create(putRecordFunction).proxy(true).build());

        Function function = createFunction(KinesisConsumeFunction.class, "handle");

        KinesisEventSource eventSource = KinesisEventSource.Builder.create(stream)
                .startingPosition(StartingPosition.TRIM_HORIZON).build();
        function.addEventSource(eventSource);
    }

    private void createSqlSample(RestApi api) {
        Queue queue = Queue.Builder.create(this, "SampleSqsQueue").build();

        Function sendFunction = createFunction(SqsFunction.class, "send");
        queue.grantSendMessages(sendFunction);
        sendFunction.addEnvironment("QUEUE_URL", queue.getQueueUrl());

        api.getRoot().addResource("sqs").addMethod("PUT",
                LambdaIntegration.Builder.create(sendFunction).proxy(true).build());

        Function receiveFunction = createFunction(SqsReceiveFunction.class, "handle");
        SqsEventSource eventSource = SqsEventSource.Builder.create(queue).build();
        receiveFunction.addEventSource(eventSource);
    }

    private void createSnsSample(RestApi api) {
        Topic topic = Topic.Builder.create(this, "SampleTopic").build();

        Function publishFunction = createFunction(SnsFunction.class, "publish");
        topic.grantPublish(publishFunction);
        publishFunction.addEnvironment("TOPIC_ARN", topic.getTopicArn());

        api.getRoot().addResource("sns").addMethod("POST",
                LambdaIntegration.Builder.create(publishFunction).proxy(true).build());

        Function subscribedFunction = createFunction(SnsSubscribedFunction.class, "handle");
        subscribedFunction.addEventSource(new SnsEventSource(topic));
    }

    private Function createFunction(Class<?> functionClass, String handler) {
        return createFunction(functionClass, handler, Duration.seconds(10), null);
    }

    private Function createFunction(Class<?> functionClass, String handler, Duration timeout, Vpc vpc) {
        Function.Builder builder = Function.Builder.create(this, functionClass.getSimpleName() + "-" + handler)
                .code(Code.fromAsset("target/assets/rapid-samples-1.0.jar"))
                .handler(String.format("%s::%s", functionClass.getCanonicalName(), handler)).runtime(Runtime.JAVA_8)
                .timeout(timeout).memorySize(256);
        if (vpc != null) {
            builder = builder.vpc(vpc).vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PRIVATE).build());
        }
        return builder.build();
    }
}
