package tpch.model;

import java.time.LocalDate;
import java.util.Objects;

public class GroupKey {

    public long orderKey;
    public LocalDate orderDate;
    public int shipPriority;

    public GroupKey() {
    }

    public GroupKey(long orderKey, LocalDate orderDate, int shipPriority) {
        this.orderKey = orderKey;
        this.orderDate = orderDate;
        this.shipPriority = shipPriority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupKey)) return false;
        GroupKey g = (GroupKey) o;
        return orderKey == g.orderKey
                && shipPriority == g.shipPriority
                && Objects.equals(orderDate, g.orderDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderKey, orderDate, shipPriority);
    }
}
