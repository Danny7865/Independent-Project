package tpch.parser;

import tpch.model.Lineitem;
import java.time.LocalDate;

public class LineitemParser {
    public static Lineitem parse(String line) {
        String[] f = line.split("\\|");
        Lineitem l = new Lineitem();
        l.l_orderkey = Long.parseLong(f[0]);
        l.l_extendedprice = Double.parseDouble(f[5]);
        l.l_discount = Double.parseDouble(f[6]);
        l.l_shipdate = LocalDate.parse(f[10]);
        return l;
    }
}
