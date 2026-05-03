@Service
@Slf4j
public class IdempotencyService {

    private static final String KEY_PREFIX  = "kafka:processed:";
    private static final Duration DEFAULT_TTL = Duration.ofHours(24);

    private final StringRedisTemplate redis;

    public IdempotencyService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Atomically mark a message ID as processed.
     *
     * @return true  — first time we've seen this ID (safe to process)
     *         false — duplicate, skip processing
     */
    public boolean markIfAbsent(String messageId) {
        return markIfAbsent(messageId, DEFAULT_TTL);
    }

    public boolean markIfAbsent(String messageId, Duration ttl) {
        try {
            Boolean isNew = redis.opsForValue()
                .setIfAbsent(KEY_PREFIX + messageId, "1", ttl);
            return Boolean.TRUE.equals(isNew);
        } catch (Exception e) {
            // Redis unavailable — fail open (allow processing) and log
            log.warn("Idempotency check failed for messageId={}, failing open: {}",
                     messageId, e.getMessage());
            return true;
        }
    }

    /** Check without marking — useful for observability / admin endpoints */
    public boolean isAlreadyProcessed(String messageId) {
        try {
            return Boolean.TRUE.equals(redis.hasKey(KEY_PREFIX + messageId));
        } catch (Exception e) {
            log.warn("Idempotency lookup failed for messageId={}: {}", messageId, e.getMessage());
            return false;
        }
    }

    /** Force-remove a key (e.g. after a manual DLT replay) */
    public void clearProcessed(String messageId) {
        redis.delete(KEY_PREFIX + messageId);
    }
}
