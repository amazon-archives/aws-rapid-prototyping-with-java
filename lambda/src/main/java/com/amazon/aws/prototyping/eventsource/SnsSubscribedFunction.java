package com.amazon.aws.prototyping.eventsource;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNS;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;

public class SnsSubscribedFunction {
    public String handle(SNSEvent event) {
        for (SNSRecord eventRecord : event.getRecords()) {
            SNS sns = eventRecord.getSNS();
            String messageId = sns.getMessageId();
            String subject = sns.getSubject();
            String message = sns.getMessage();
            System.out.printf("messageId=%s, subject=%s, message=%s\n", messageId, subject, message);
        }

        return "ok";
    }
}
