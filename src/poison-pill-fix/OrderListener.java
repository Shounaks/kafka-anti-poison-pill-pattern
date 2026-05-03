@Component
@Slf4j
@RequiredArgsConstructor
public class OrderListener {

    private final OrderService       orderService;
    private final IdempotencyService idempotency;

    @RetryableTopic(
        attempts  = "3",
        backoff   = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 30_000),
        autoCreateTopics         = "false",               // manage topics explicitly
        topicSuffixingStrategy   = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltStrategy              = DltStrategy.FAIL_ON_ERROR,
        include = { TransientDataAccessException.class,   // only retry these
                    ResourceAccessException.class }
    )
    @KafkaListener(
        topics  = "orders",
        groupId = "order-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(
            Order order,
            @Header(KafkaHeaders.RECEIVED_TOPIC)     String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int    partition,
            @Header(KafkaHeaders.OFFSET)             long   offset,
            @Header(KafkaHeaders.RECEIVED_KEY)       String key) {

        // ── Structured logging context (visible in every log line below) ──
        MDC.put("orderId",   order.getId());
        MDC.put("topic",     topic);
        MDC.put("partition", String.valueOf(partition));
        MDC.put("offset",    String.valueOf(offset));

        try {
            log.info("Received message");

            // ── 1. Idempotency gate ───────────────────────────────────────
            String messageId = buildMessageId(topic, partition, offset);
            if (!idempotency.markIfAbsent(messageId)) {
                log.warn("Duplicate message detected — skipping. messageId={}", messageId);
                return; // commit offset, do nothing
            }

            // ── 2. Validate before any business logic ─────────────────────
            validateOrder(order);   // throws ValidationException (non-retryable)

            // ── 3. Business logic ─────────────────────────────────────────
            orderService.process(order);
            log.info("Order processed successfully");

        } catch (ValidationException e) {
            // Non-retryable — log and let the error handler route to DLT
            log.error("Validation failed, routing to DLT: {}", e.getMessage());
            throw e;

        } catch (Exception e) {
            // Retryable — Spring-Kafka will honour the @RetryableTopic config
            log.error("Transient error processing order, will retry: {}", e.getMessage());
            throw e;

        } finally {
            MDC.clear();
        }
    }

    // ── DLT handler ───────────────────────────────────────────────────────
    @DltHandler
    public void handleDlt(
            Order order,
            @Header(KafkaHeaders.RECEIVED_TOPIC)     String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int    partition,
            @Header(KafkaHeaders.OFFSET)             long   offset,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE)  String exceptionMessage) {

        MDC.put("dltTopic",   topic);
        MDC.put("partition",  String.valueOf(partition));
        MDC.put("offset",     String.valueOf(offset));

        try {
            log.error("Message exhausted all retries. exception={} order={}",
                      exceptionMessage, order);

            // Persist to an audit table so ops can replay manually
            orderService.saveToDltAudit(order, topic, exceptionMessage);

            // (Optional) fire an alert — PagerDuty, Slack, etc.
            // alertService.sendDltAlert(order, topic, exceptionMessage);

        } finally {
            MDC.clear();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /** Kafka's (topic, partition, offset) triple is a natural unique message ID */
    private String buildMessageId(String topic, int partition, long offset) {
        return String.format("%s-%d-%d", topic, partition, offset);
    }

    private void validateOrder(Order order) {
        if (order == null)               throw new ValidationException("Order is null");
        if (order.getId() == null)       throw new ValidationException("Order ID missing");
        if (order.getAmount() == null)   throw new ValidationException("Amount missing");
        if (order.getAmount() < 0)       throw new ValidationException("Negative amount");
        if (order.getCustomerId() == null) throw new ValidationException("Customer ID missing");
    }
}
