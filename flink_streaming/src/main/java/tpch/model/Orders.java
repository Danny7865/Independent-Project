package tpch.model;

import java.time.LocalDate;

public class Orders {
    public long o_orderkey;
    public long o_custkey;
    public LocalDate o_orderdate;
    public int o_shippriority;
}
