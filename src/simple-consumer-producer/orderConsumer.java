@Component
@RequiredArgsConstructor
public class OrderConsumer {

    private final OrderService orderService;

    @KafkaListener(
        topics = "orders",
        groupId = "my-app-group",
        concurrency = "3"          // 3 threads = 3 partitions consumed in parallel
    )
    public void consume(
            ConsumerRecord<String, OrderEvent> record,
            Acknowledgment ack) {

        log.info("Received: partition={} offset={} key={}",
            record.partition(), record.offset(), record.key());

        try {
            orderService.process(record.value());
            ack.acknowledge();    // ✅ commit offset ONLY on success
        } catch (Exception e) {
            // don't ack — let error handler decide
            throw e;
        }
    }
}