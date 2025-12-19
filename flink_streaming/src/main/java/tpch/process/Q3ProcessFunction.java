package tpch.process;

import tpch.model.*;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * TPC-H Q3 Streaming Incremental Processor (FIFO workload)
 *
 * Q3:
 *  c_mktsegment = 'BUILDING'
 *  o_orderdate < DATE
 *  l_shipdate  > DATE
 *
 * Output: Δ revenue per (orderkey, orderdate, shippriority)
 */
public class Q3ProcessFunction
        extends KeyedProcessFunction<Long, UpdateEvent<?>, String> {

    // ---------------- Q3 PARAMETERS ----------------
    private static final String SEGMENT = "BUILDING";
    private static final LocalDate DATE = LocalDate.of(1995, 3, 15);

    // Fixed experiment config
    private static final double DATA_GB = 1.0;

    // ---------------- STATE ----------------
    private MapState<Long, Customer> customers;           // c_custkey -> Customer
    private MapState<Long, Orders> orders;               // o_orderkey -> Orders
    private MapState<Long, List<Lineitem>> lineitems;    // o_orderkey -> List<Lineitem>
    private MapState<GroupKey, Double> revenue;          // (orderkey, orderdate, shippriority) -> revenue

    // interval verification config
    private long eventId  = 0;
    private static final long VERIFY_INTERVAL = 10_000;
    // Floating-point comparison tolerance
    private static final double EPS = 1e-9;
    // Performance metrics
    private long startTime;
    private boolean finished = false;

    // ---------------- INIT ----------------
    @Override
    public void open(Configuration parameters) {

        customers = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "customers",
                        Types.LONG,
                        Types.POJO(Customer.class)
                )
        );

        orders = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "orders",
                        Types.LONG,
                        Types.POJO(Orders.class)
                )
        );

        lineitems = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "lineitems",
                        Types.LONG,
                        Types.LIST(Types.POJO(Lineitem.class))
                )
        );

        revenue = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "revenue",
                        Types.POJO(GroupKey.class),
                        Types.DOUBLE
                )
        );

        startTime = System.currentTimeMillis();
    }

    // ---------------- MAIN ENTRY ----------------
    @Override
    public void processElement(
            UpdateEvent<?> event,
            Context ctx,
            Collector<String> out) throws Exception {

        Object t = event.tuple;

        if (t instanceof Customer) {
            handleCustomer((UpdateEvent<Customer>) event);
        } else if (t instanceof Orders) {
            handleOrders((UpdateEvent<Orders>) event);
        } else if (t instanceof Lineitem) {
            handleLineitem((UpdateEvent<Lineitem>) event, out);
        }

        // global event counter
        eventId++;

        // periodic snapshot
        if (eventId % VERIFY_INTERVAL == 0) {
            emitSnapshot();
        }
    }

    // ============================================================
    // CUSTOMER
    // ============================================================
    private void handleCustomer(UpdateEvent<Customer> e) throws Exception {
        Customer c = e.tuple;
        if (!SEGMENT.equals(c.c_mktsegment)) return;
        if (e.op == UpdateEvent.Op.INSERT) {
            customers.put(c.c_custkey, c);
        }
    }

    // ============================================================
    // ORDERS
    // ============================================================
    private void handleOrders(UpdateEvent<Orders> e) throws Exception {
        Orders o = e.tuple;
        if (!o.o_orderdate.isBefore(DATE)) return;
        if (customers.get(o.o_custkey) == null) return; // 修正 contains
        if (e.op == UpdateEvent.Op.INSERT) {
            orders.put(o.o_orderkey, o);
        }
    }

    // ============================================================
    // LINEITEM (FACT TABLE, FIFO)
    // ============================================================
    private void handleLineitem(
            UpdateEvent<Lineitem> e,
            Collector<String> out) throws Exception {

        Lineitem l = e.tuple;
        if (!l.l_shipdate.isAfter(DATE)) return;
        long orderKey = l.l_orderkey;
        if (orders.get(orderKey) == null) return; // 修正 contains

        // Initialize list if absent (MapState has no putIfAbsent)
        if (lineitems.get(orderKey) == null) {
            lineitems.put(orderKey, new ArrayList<>());
        }

        List<Lineitem> list = lineitems.get(orderKey);
        Orders o = orders.get(orderKey);

        GroupKey gk = new GroupKey(
                o.o_orderkey,
                o.o_orderdate,
                o.o_shippriority
        );

        double delta = l.l_extendedprice * (1.0 - l.l_discount);
        double oldRevenue = revenue.get(gk) == null ? 0.0 : revenue.get(gk);
        double newRevenue;

        if (e.op == UpdateEvent.Op.INSERT) {
            list.add(l);
            newRevenue = oldRevenue + delta;
        } else {
            list.remove(l);
            newRevenue = oldRevenue - delta;
        }

        // Fix negative zero: treat near-zero values as 0 and remove from state
        if (Math.abs(newRevenue) < EPS) {
            newRevenue = 0.0;
            revenue.remove(gk);
        } else {
            revenue.put(gk, newRevenue);
        }

        // ---------------- Δ OUTPUT ----------------
        out.collect(String.format("[Q3 UPDATE] orderKey=%d, date=%s, priority=%d, oldRevenue=%.2f, newRevenue=%.2f",
                gk.orderKey, gk.orderDate, gk.shipPriority, oldRevenue, newRevenue));
    }

    // ---------------- EMIT SNAPSHOT ----------------
    private void emitSnapshot() throws IOException, Exception {
        File logFile = new File("logs/flink_snapshot.log");
        if (!logFile.getParentFile().exists()) {
            logFile.getParentFile().mkdirs();
        }

        try (FileWriter fw = new FileWriter(logFile, true)) {
            // Use entries() to iterate MapState
            for (Map.Entry<GroupKey, Double> entry : revenue.entries()) {
                GroupKey gk = entry.getKey();
                double rev = entry.getValue();
                fw.write(String.format(
                        "SNAPSHOT,event=%d,orderKey=%d,date=%s,priority=%d,revenue=%.6f%n",
                        eventId, gk.orderKey, gk.orderDate, gk.shipPriority, rev
                ));
            }
        }
    }
}
