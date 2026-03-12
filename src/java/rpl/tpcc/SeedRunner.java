package rpl.tpcc;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class SeedRunner {

    public static void run(String conductorHost, int numWarehouses, int numTasks, int clientIndex, int totalClients) {
        long maxTickets = (1500L * numTasks) / totalClients;

        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("rpl.tpcc.seed"));

        IFn runSeed = Clojure.var("rpl.tpcc.seed", "run-seed!");
        runSeed.invoke(conductorHost, numWarehouses, clientIndex, totalClients, maxTickets);
    }

    public static void main(String[] args) {
        String tpccArgs = System.getenv("TPCC_ARGS");
        if (tpccArgs == null || tpccArgs.isEmpty()) {
            System.err.println("TPCC_ARGS env var required: <conductor-host> <num-warehouses> <num-tasks> <client-index> <total-clients>");
            System.exit(1);
        }
        String[] parts = tpccArgs.trim().split("\\s+");
        if (parts.length != 5) {
            System.err.println("TPCC_ARGS requires 5 values: <conductor-host> <num-warehouses> <num-tasks> <client-index> <total-clients>");
            System.exit(1);
        }

        run(parts[0],
            Integer.parseInt(parts[1]),
            Integer.parseInt(parts[2]),
            Integer.parseInt(parts[3]),
            Integer.parseInt(parts[4]));

        Runtime.getRuntime().halt(0);
    }
}
