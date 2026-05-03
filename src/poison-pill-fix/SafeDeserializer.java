public class SafeDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> targetType;

    public SafeDeserializer(Class<T> targetType) {
        this.objectMapper = new ObjectMapper();
        this.targetType = targetType;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, targetType);
        } catch (Exception e) {
            // Wrap in a runtime so ErrorHandlingDeserializer can intercept it
            // — the raw bytes are preserved in the exception for DLT forwarding
            throw new RuntimeException(
                String.format("Failed to deserialize message from topic [%s]: %s", topic, new String(data)), e
            );
        }
    }
}
