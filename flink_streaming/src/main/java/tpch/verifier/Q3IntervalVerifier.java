package tpch.verifier;

import tpch.model.*;
import tpch.parser.*;

import java.io.*;
import java.time.LocalDate;
import java.util.*;

/**
 * Offline verifier for TPC-H Q3
 * Full replay + interval snapshot verification
 */
public class Q3IntervalVerifier {

    private static final String SEGMENT = "BUILDING";
    private static final LocalDate DATE = LocalDate.of(1995, 3, 15);
    private static final long VERIFY_INTERVAL = 50_000;

    // Reference state
    private static final Map<Long, Customer> customers = new HashMap<>();
    private static final Map<Long, Orders> orders = new HashMap<>();
    private static final Map<GroupKey, Double> revenue = new HashMap<>();

    private static long eventId = 0; // 事件计数
    private static long compared = 0;
    private static double sumError = 0.0;
    private static double maxError = 0.0;
    private static long totalCompared = 0;
    private static double totalSumError = 0.0;
    private static double totalMaxError = 0.0;


    public static void main(String[] args) throws Exception {

        // 1. Replay customer
        replayFile("customer.tbl", Q3IntervalVerifier::handleCustomer);

        // 2. Replay orders
        replayFile("orders.tbl", Q3IntervalVerifier::handleOrders);

        // 3. Replay lineitem FIFO
        replayLineitemFIFO("lineitem.tbl");

        System.out.printf("[FINAL VERIFY] totalCompared=%d avgError=%.6e maxError=%.6e",
                totalCompared,
                totalSumError / Math.max(1, totalCompared),
                totalMaxError);

    }

    // ================= Replay Helpers =================
    private static void replayFile(String file, java.util.function.Consumer<String> handler) throws Exception {
        BufferedReader br = reader(file);
        String line;
        while ((line = br.readLine()) != null) {
            handler.accept(line);
            tick();
        }
    }

    private static void replayLineitemFIFO(String file) throws Exception {
        BufferedReader br = reader(file);
        Queue<Lineitem> fifo = new ArrayDeque<>();
        String line;

        while ((line = br.readLine()) != null) {
            Lineitem l = LineitemParser.parse(line);
            handleLineitem(l, true);
            fifo.add(l);
            tick();

            if (fifo.size() > 1000) { // FIFO
                Lineitem old = fifo.poll();
                handleLineitem(old, false);
                tick();
            }
        }
    }

    private static void tick() throws Exception {
        eventId++;
        if (eventId % VERIFY_INTERVAL == 0) {
            verifySnapshot(); // interval verification
        }
    }

    // ================= Handlers =================
    private static void handleCustomer(String line) {
        Customer c = CustomerParser.parse(line);
        if (SEGMENT.equals(c.c_mktsegment)) {
            customers.put(c.c_custkey, c);
        }
    }

    private static void handleOrders(String line) {
        Orders o = OrdersParser.parse(line);
        if (!o.o_orderdate.isBefore(DATE)) return;
        if (!customers.containsKey(o.o_custkey)) return;
        orders.put(o.o_orderkey, o);
    }

    private static void handleLineitem(Lineitem l, boolean insert) {
        if (!l.l_shipdate.isAfter(DATE)) return;
        Orders o = orders.get(l.l_orderkey);
        if (o == null) return;

        GroupKey gk = new GroupKey(o.o_orderkey, o.o_orderdate, o.o_shippriority);
        double delta = l.l_extendedprice * (1.0 - l.l_discount);
        double old = revenue.getOrDefault(gk, 0.0);
        double now = insert ? old + delta : old - delta;

        if (Math.abs(now) < 1e-9) {
            revenue.remove(gk);
        } else {
            revenue.put(gk, now);
        }
    }

    // ================= Verification =================
    private static void verifySnapshot() throws Exception {
        Map<GroupKey, Double> flinkSnapshot = StreamingSnapshotLoader.load(eventId);

        if (flinkSnapshot.isEmpty()) {
            System.out.printf("[VERIFY] event=%d no snapshot found%n", eventId);
            return;
        }

        for (Map.Entry<GroupKey, Double> e : revenue.entrySet()) {
            double ref = e.getValue();
            double stream = flinkSnapshot.getOrDefault(e.getKey(), 0.0);
            double err = Math.abs(ref - stream);
            compared++;
            sumError += err;
            maxError = Math.max(maxError, err);
        }

        totalCompared += compared;
        totalSumError += sumError;
        totalMaxError = Math.max(totalMaxError, maxError);

        System.out.printf("[VERIFY] event=%d compared=%d avgError=%.6e maxError=%.6e%n",
                eventId, compared, sumError / Math.max(1, compared), maxError);
    }

    // ================= Snapshot Loader =================
    static class StreamingSnapshotLoader {
        public static Map<GroupKey, Double> load(long eventId) {
            Map<GroupKey, Double> map = new HashMap<>();
            try (BufferedReader br = new BufferedReader(new FileReader("logs/flink_snapshot.log"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.startsWith("SNAPSHOT")) continue;
                    String[] p = line.split(",");
                    long e = Long.parseLong(p[1].split("=")[1]);
                    if (e != eventId) continue;

                    long orderKey = Long.parseLong(p[2].split("=")[1]);
                    LocalDate date = LocalDate.parse(p[3].split("=")[1]);
                    int pri = Integer.parseInt(p[4].split("=")[1]);
                    double rev = Double.parseDouble(p[5].split("=")[1]);
                    map.put(new GroupKey(orderKey, date, pri), rev);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return map;
        }
    }

    private static BufferedReader reader(String file) {
        return new BufferedReader(new InputStreamReader(
                Q3IntervalVerifier.class.getClassLoader().getResourceAsStream(file)
        ));
    }
}
