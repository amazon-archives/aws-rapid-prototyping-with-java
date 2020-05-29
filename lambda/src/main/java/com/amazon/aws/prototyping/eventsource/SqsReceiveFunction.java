package com.amazon.aws.prototyping.eventsource;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

public class SqsReceiveFunction {
    public String handle(SQSEvent event) {
        for (SQSMessage message : event.getRecords()) {
            String messageId = message.getMessageId();
            String body = message.getBody();
            System.out.printf("messageId=%s, body=%s\n", messageId, body);
        }
        return "ok";
    }
}
