@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    // Fire-and-forget (still async)
    public void send(OrderEvent event) {
        kafkaTemplate.send("orders", event.getId(), event);
    }

    // With result callback — know exactly what happened
    public CompletableFuture<SendResult<String, OrderEvent>> sendWithCallback(OrderEvent event) {
        return kafkaTemplate.send("orders", event.getId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send message: {}", ex.getMessage());
                } else {
                    log.info("Sent to partition={} offset={}",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
    }

    // Transactional — either all messages land or none do
    @Transactional("kafkaTransactionManager")
    public void sendBatch(List<OrderEvent> events) {
        events.forEach(e -> kafkaTemplate.send("orders", e.getId(), e));
    }
}