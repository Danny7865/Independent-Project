package tpch.source;

import tpch.model.*;
import tpch.parser.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.util.*;

public class TPCHFIFOStreamingSource
        implements SourceFunction<UpdateEvent<?>> {

    private static final int WINDOW_SIZE = 1000;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<UpdateEvent<?>> ctx) throws Exception {

        // Insert Customer
        BufferedReader cbr = new BufferedReader(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("customer.tbl")
        ));
        String line;
        while ((line = cbr.readLine()) != null) {
            ctx.collect(new UpdateEvent<>(UpdateEvent.Op.INSERT,
                    CustomerParser.parse(line)));
        }

        // 2. Insert Orders
        BufferedReader obr = new BufferedReader(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("orders.tbl")
        ));
        while ((line = obr.readLine()) != null) {
            ctx.collect(new UpdateEvent<>(UpdateEvent.Op.INSERT,
                    OrdersParser.parse(line)));
        }

        // 3. Lineitem FIFO streaming
        BufferedReader lbr = new BufferedReader(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("lineitem.tbl")
        ));

        Queue<Lineitem> fifo = new ArrayDeque<>();

        while (running && (line = lbr.readLine()) != null) {

            Lineitem l = LineitemParser.parse(line);

            ctx.collect(new UpdateEvent<>(UpdateEvent.Op.INSERT, l));
            fifo.add(l);

            if (fifo.size() > WINDOW_SIZE) {
                Lineitem old = fifo.poll();
                ctx.collect(new UpdateEvent<>(UpdateEvent.Op.DELETE, old));
            }

            //Thread.sleep(1);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
