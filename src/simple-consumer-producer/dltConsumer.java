@Component
public class DltConsumer {

    @KafkaListener(topics = "orders.DLT", groupId = "dlt-inspector")
    public void handleDlt(
            ConsumerRecord<String, OrderEvent> record,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMsg,
            @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
            @Header(KafkaHeaders.ORIGINAL_OFFSET) long originalOffset,
            Acknowledgment ack) {

        log.error("""
            DLT message from topic={} offset={}
            Reason: {}
            Payload: {}
            """, originalTopic, originalOffset, exceptionMsg, record.value());

        // Persist to DB for ops team review / manual replay dashboard
        deadLetterRepository.save(new DeadLetterRecord(record, exceptionMsg));

        alertingService.criticalAlert("DLT consumer failed", rec, ex);
        emergencyRepository.saveRaw(rec.topic(), rec.key(), rec.value(), ex);
        ack.acknowledge();
    }
}