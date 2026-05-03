@Configuration
public class DefaultErrorHandler{
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, OrderEvent> kafkaTemplate) {

        // Exponential: 1s → 2s → 4s → 8s (4 attempts total)
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        // Where to send messages that exhaust all retries
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            // Convention: orders → orders.DLT
            (rec, ex) -> new TopicPartition(rec.topic() + ".DLT", rec.partition())
        );

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Don't retry on unrecoverable exceptions — send straight to DLT
        handler.addNotRetryableExceptions(
            InvalidMessageException.class,
            JsonProcessingException.class
        );
    

        return handler;
    }

    // Wire it into the listener container factory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> cf,
            DefaultErrorHandler errorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
        factory.setConsumerFactory(cf);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}