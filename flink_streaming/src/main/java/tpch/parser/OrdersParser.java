package tpch.parser;

import tpch.model.Orders;
import java.time.LocalDate;

public class OrdersParser {

    public static Orders parse(String line) {
        String[] f = line.split("\\|");

        Orders o = new Orders();
        o.o_orderkey = Long.parseLong(f[0]);
        o.o_custkey = Long.parseLong(f[1]);
        o.o_orderdate = LocalDate.parse(f[4]);
        o.o_shippriority = Integer.parseInt(f[7]);

        return o;
    }
}
