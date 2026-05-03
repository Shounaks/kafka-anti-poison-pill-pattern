@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ──────────────────────────────────────────────
    // Consumer factory — wraps your deserializer so
    // corrupt bytes go to DLT instead of crashing
    // ──────────────────────────────────────────────
    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // manual ack

        // ErrorHandlingDeserializer intercepts RecordDeserializationException
        // so a single bad message never blocks the entire partition
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, SafeDeserializer.class);   // <-- our wrapper above

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> //For Async Processing
           kafkaListenerContainerFactory(
               ConsumerFactory<String, Order> cf,
               KafkaTemplate<String, Order> kt) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, Order>();
        factory.setConsumerFactory(cf);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        // DeadLetterPublishingRecoverer sends unrecoverable records straight to
        // <topic>.DLT — deserialization failures land here immediately (no retries)
        var recoverer = new DeadLetterPublishingRecoverer(kt,
            (record, ex) -> {
                log.error("Sending poison pill to DLT. topic={} offset={} exception={}",
                    record.topic(), record.offset(), ex.getMessage());
                return new TopicPartition(record.topic() + ".DLT", record.partition());
            });

        // Back-off: 1 s, 2 s, 4 s — then hand off to recoverer (DLT)
        var backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);

        var errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // These exceptions are NOT retried — go straight to DLT
        errorHandler.addNotRetryableExceptions(
            JsonParseException.class,         // malformed JSON
            InvalidFormatException.class,     // wrong field type
            ValidationException.class,        // bean validation failure
            BusinessRuleException.class       // your own domain exceptions
        );

        // These transient errors ARE retried
        errorHandler.addRetryableExceptions(
            TransientDataAccessException.class,
            ResourceAccessException.class
        );

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    // ──────────────────────────────────────────────
    // @RetryableTopic global config (overrides per-annotation defaults for all listeners in app)
    // ──────────────────────────────────────────────
    @Bean
    public RetryTopicConfiguration retryTopicConfig(KafkaTemplate<String, Order> kt) {
        return RetryTopicConfigurationBuilder
            .newInstance()
            .fixedBackOff(2_000L)
            .maxAttempts(3)
            .useSingleTopicForFixedDelays()             // one retry topic, not N
            .doNotRetryOnDltFailure()
            .dltHandlerMethod("handleDlt")
            .create(kt);
    }
}
