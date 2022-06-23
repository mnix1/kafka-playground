package mnix.kafka.bankbalance;

import java.time.Instant;

public class BalanceChange {
    private String name;
    private Long amount;
    private Instant timestamp;

    public BalanceChange(String name, Long amount, Instant timestamp) {
        this.name = name;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public Long getAmount() {
        return amount;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public BalanceChange merge(BalanceChange other) {
        return new BalanceChange(name, amount + other.amount, timestamp.isAfter(other.timestamp) ? timestamp : other.timestamp);
    }
}
