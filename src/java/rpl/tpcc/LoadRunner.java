package rpl.tpcc;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class LoadRunner {

    public static void run(String conductorHost, int numWarehouses, int durationMinutes, int clientIndex, int totalClients) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("rpl.tpcc.load-runner"));

        IFn makeCtx = Clojure.var("rpl.tpcc.load-runner", "make-load-context");
        Object loadCtx = makeCtx.invoke(conductorHost);

        IFn runLoad = Clojure.var("rpl.tpcc.load-runner", "run-load!");
        IFn runLoadPrint = Clojure.var("rpl.tpcc.load-runner", "run-load-print!");
        Object quietKey = Clojure.read(":quiet?");

        System.out.println("=== Warmup run (10 minutes) ===");
        runLoad.invoke(loadCtx, numWarehouses, 600, clientIndex, totalClients, quietKey, true);

        System.out.println("=== Measurement run (" + durationMinutes + " minutes) ===");
        runLoadPrint.invoke(loadCtx, numWarehouses, durationMinutes * 60 + 180, clientIndex, totalClients);
    }

    public static void main(String[] args) {
        String tpccArgs = System.getenv("TPCC_ARGS");
        if (tpccArgs == null || tpccArgs.isEmpty()) {
            System.err.println("TPCC_ARGS env var required: <conductor-host> <num-warehouses> <duration-minutes> <client-index> <total-clients>");
            System.exit(1);
        }
        String[] parts = tpccArgs.trim().split("\\s+");
        if (parts.length != 5) {
            System.err.println("TPCC_ARGS requires 5 values: <conductor-host> <num-warehouses> <duration-minutes> <client-index> <total-clients>");
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
