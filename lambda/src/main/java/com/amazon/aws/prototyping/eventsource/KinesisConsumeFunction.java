package com.amazon.aws.prototyping.eventsource;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class KinesisConsumeFunction {
    public String handle(KinesisEvent event) {
        for (KinesisEventRecord eventRecord : event.getRecords()) {
            Record record = eventRecord.getKinesis();
            String sequenceNumber = record.getSequenceNumber();
            String partitionKey = record.getPartitionKey();
            String data = new String(record.getData().array());
            System.out.printf("sequenceNumber=%s, partitionKey=%s, data=%s\n", sequenceNumber, partitionKey, data);
        }

        return "ok";
    }
}
