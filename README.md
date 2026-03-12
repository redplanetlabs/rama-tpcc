# tpcc-benchmark

A [TPC-C](http://www.tpc.org/tpcc/) benchmark implementation for [Rama](https://redplanetlabs.com). TPC-C is the industry-standard OLTP benchmark, simulating a warehouse order-processing workload with five transaction types (three read-write, two read-only) and strict consistency requirements. The primary metric is "tpmC", new-order transactions per minute. This implementation achieves 1.68M tpmC at 95% efficiency, matching CockroachDB's [published result](https://www.cockroachlabs.com/docs/stable/performance-benchmarking-with-tpcc-local) but on fewer and cheaper nodes — 64 `i8g.4xlarge` vs. 81 `c5d.9xlarge`, for 40% lower cost.

See [this blog post](TODO) for the full results, including detailed latency numbers and an overview on how Rama performs so well.

### Cost comparison with CockroachDB

CockroachDB published an [unofficial TPC-C benchmark](https://www.cockroachlabs.com/docs/stable/performance-benchmarking-with-tpcc-local) on 140,000 warehouses.

| | CockroachDB | Rama |
|---|---|---|
| **Nodes** | 81 x c5d.9xlarge | 64 x i8g.4xlarge |
| **Cluster cost/hr** | $139.97 | $87.81 |
| **vCPUs (total)** | 2,916 | 1,024 |
| **Warehouses** | 140,000 | 140,000 |
| **RF** | 3 | 3 |
| **tpmC** | 1,684,437 | 1,676,800 |
| **Efficiency** | 95.5% | 95.0% |
| **Cost per tpmC** | $0.0000831/hr | $0.0000524/hr |

**Rama is 40% cheaper per tpmC** at comparable throughput and efficiency. Both benchmarks use the full 10 terminals per warehouse and RF=3.

### Cost comparison with YugabyteDB

YugabyteDB published a [TPC-C benchmark](https://docs.yugabyte.com/stable/benchmark/tpcc/high-scale-workloads/) claiming 1,000,000 tpmC on 150,000 warehouses at "99.8% efficiency" on 75 x c5d.12xlarge nodes (RF=3).

| | YugabyteDB | Rama |
|---|---|---|
| **Nodes** | 75 x c5d.12xlarge | 64 x i8g.4xlarge |
| **Cluster cost/hr** | $172.80 | $87.81 |
| **vCPUs (total)** | 3,600 | 1,024 |
| **Warehouses** | 150,000 | 140,000 |
| **RF** | 3 | 3 |
| **tpmC** | 1,000,000 | 1,676,800 |
| **Efficiency** | 52.9% (see below) | 95.0% |
| **Cost per tpmC** | $0.0001728/hr | **$0.0000524/hr** |

**Rama is 70% cheaper per tpmC.**

YugabyteDB's "99.8% efficiency" claim is misleading. With 150,000 warehouses and the standard 10 terminals per warehouse, the theoretical maximum is ~1,890,000 tpmC. Their actual 1,000,000 tpmC is only **52.9% of full terminal capacity**. They achieved "99.8% efficiency" by running roughly 5.3 terminals per warehouse instead of the standard 10, then reporting efficiency against that reduced terminal count. By contrast, both the Rama and CockroachDB benchmarks use the full 10 terminals per warehouse.

## Running the benchmark

### Prerequisites

- A [Rama](https://redplanetlabs.com) cluster (see [Rama documentation](https://redplanetlabs.com/docs/~/operating-rama.html) for setup)
- Download `rama-tpcc-1.0.0.jar` from https://github.com/redplanetlabs/tpcc-benchmark/releases

### 1. Cluster setup

Deploy a 65-node Rama cluster with `i8g.4xlarge` instances for worker nodes. Mount the NVMe disks on the nodes with XFS.

**Set `jute.maxbuffer` for ZooKeeper, Conductor, and Supervisor:**

All three processes need the ZooKeeper client buffer size increased to 4MB:

- **Conductor**: In `rama.yaml` on the conductor node, add to `conductor.child.opts`:
  ```
  conductor.child.opts: "-Djute.maxbuffer=4194304"
  ```
- **Supervisor**: In `rama.yaml` on each worker node, add to `supervisor.child.opts`:
  ```
  supervisor.child.opts: "-Djute.maxbuffer=4194304"
  ```
- **ZooKeeper**: ZooKeeper is deployed independently from Rama. [`jute.maxbuffer` must be set as a Java system property](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html). Add it via `SERVER_JVMFLAGS` in your ZooKeeper installation.

**Configure isolation scheduler:**

Add this to the Conductor's `rama.yaml` and restart the Conductor process:

```
conductor.assignment.mode:
  type: isolation
  modules:
    monitoring: 1
    rpl.tpcc/TPCCModule: 64
```

**Deploy the monitoring module:**

```bash
rama deploy \
  --action launch \
  --module com.rpl/MonitoringModule \
  --tasks 16 \
  --threads 16 \
  --workers 1 \
  --configOverrides monitoring-deploy.yaml
```


### 2. Build and deploy the module

```bash
rama deploy \
--action launch \
--module rpl.tpcc/TPCCModuleWithSeed \
--jar rama-tpcc-1.0.0.jar \
--useInternalHostnames \
--tasks 512 \
--threads 512 \
--workers 64 \
--moduleOptions 'topology.microbatch.phase.timeout.seconds=300;pstate.proxy.limit=20000;depot.microbatch.max.records=1500;topology.microbatch.ack.branching.factor=512;topology.microbatch.ack.delay.base.millis=15;topology.microbatch.ack.delay.step.millis=15' \
--configOverrides deploy.yaml
```


### 3. Prepare client nodes

Set up at least 4 client nodes in the same VPC as the Rama cluster. On each client node:

1. Unpack the Rama release
2. Place [rama-tpcc-1.0.0.jar](https://github.com/redplanetlabs/tpcc-benchmark/releases) in the same directory as the unpacked Rama release

The seed and load runners are run from these nodes using the `rama` command.

### 4. Load seed data

Pause the `tpcc` microbatch topology in the cluster UI first – it's faster to bulk-append all records before processing them.

From each client node, run the seed runner (adjusting `<client-index>` 0 through 3 for each node):

```bash
TPCC_ARGS="<conductor-host> 140000 512 <client-index> <total-clients>" \
  ./rama runJava rpl.tpcc.SeedRunner rama-tpcc-1.0.0.jar
```

Args: `<conductor-host> <num-warehouses> <num-tasks> <client-index> <total-clients>`

Once all seed clients finish (after about 5 minutes), unpause the `tpcc` topology and wait for it to process all records (takes roughly 1.5 hours at 140K warehouses). Processing is complete when "size" and "progress" are equal for all depots in the "Microbatch per-depot entry processing progress" chart for the `tpcc` topology in the module's cluster UI page, which can be accessed at `http://<conductor-host>:8888`.

### 5. Remove seed depots

After seed processing completes, update the module to remove the seed-specific depots and processing logic. This eliminates unnecessary overhead during the benchmark run.

```bash
rama deploy \
  --action update \
  --module rpl.tpcc/TPCCModule \
  --jar rama-tpcc-1.0.0.jar \
  --useInternalHostnames \
  --objectsToDelete '*item-depot,*warehouse-depot,*district-depot,*customer-depot,*stock-depot' \
  --configOverrides deploy.yaml
```

### 6. Add replication

After seed processing completes, scale up to RF=3. This is faster than seeding with replication enabled from the start.

```bash
rama scaleExecutors \
  --module rpl.tpcc/TPCCModule \
  --replicationFactor 3
```

This takes about one hour to complete. Wait for the "ISR count" chart in the module telemetry to reach 3 and for the module to stop rebalancing leadership.

### 7. Run the benchmark

From each client node, run the load runner (adjusting `<client-index>` 0 through 3 for each node):

```bash
TPCC_ARGS="<conductor-host> 140000 120 <client-index> <total-clients>" \
  ./rama runJava rpl.tpcc.LoadRunner rama-tpcc-1.0.0.jar
```

Args: `<conductor-host> <num-warehouses> <duration-minutes> <client-index> <total-clients>`

The load runner automatically performs a 10-minute warmup (with no output) before the measurement run. Periodic progress reports and the final summary are printed to stdout. Results are also appended to the file `LOAD-RUNNER-RESULTS` when the run completes.
