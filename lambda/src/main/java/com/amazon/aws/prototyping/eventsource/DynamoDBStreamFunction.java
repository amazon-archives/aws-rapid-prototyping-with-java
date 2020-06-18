package com.amazon.aws.prototyping.eventsource;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;

public class DynamoDBStreamFunction {
    public String handle(DynamodbEvent event) {
        for (DynamodbStreamRecord eventRecord : event.getRecords()) {
            StreamRecord streamRecord = eventRecord.getDynamodb();

            String sequenceNumber = streamRecord.getSequenceNumber();
            Map<String, AttributeValue> oldImage = streamRecord.getOldImage();
            Map<String, AttributeValue> newImage = streamRecord.getNewImage();

            System.out.printf("sequenceNumber=%s, oldImage=%s, newImage=%s\n", sequenceNumber, oldImage, newImage);
        }
        return "ok";
    }
}
