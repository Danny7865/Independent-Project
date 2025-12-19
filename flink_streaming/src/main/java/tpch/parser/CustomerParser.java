package tpch.parser;

import tpch.model.Customer;

public class CustomerParser {

    public static Customer parse(String line) {
        String[] f = line.split("\\|");

        Customer c = new Customer();
        c.c_custkey = Long.parseLong(f[0]);
        c.c_mktsegment = f[6];

        return c;
    }
}
