(ns rpl.tpcc.load-runner
  "Client-side TPC-C load generator using the terminal model.
   Each warehouse has one terminal per district, cycling independently:
   pick tx → keying delay → submit async → wait for response → think delay → repeat."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [rpl.tpcc.datagen :as dg]
            [rpl.tpcc.helpers :as h])
  (:import [com.tdunning.math.stats MergingDigest]
           [java.util ArrayList Collections]
           [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue ScheduledThreadPoolExecutor Semaphore
                                 TimeUnit atomic.AtomicBoolean atomic.AtomicInteger]
           [java.util.function BiConsumer Consumer]
           [rpl.tpcc.types NewOrderInvalidResult PaymentByLastTx]))

(def ^:private module-name "rpl.tpcc/TPCCModule")

;; ---------------------------------------------------------------------------
;; Constants
;; ---------------------------------------------------------------------------

(def KEYING-TIMES
  {:new-order 18.0, :payment 3.0, :order-status 2.0, :delivery 2.0, :stock-level 2.0})

(def THINK-TIME-MEANS
  {:new-order 12.0, :payment 12.0, :order-status 10.0, :delivery 5.0, :stock-level 5.0})

(defn- exponential-think-time
  "Generate think time: Tt = -ln(r) * mean, capped at 10*mean."
  ^double [^double mean]
  (let [r (Math/max 1e-15 (double (rand)))]
    (Math/min (* -1.0 (Math/log r) mean)
              (* 10.0 mean))))

;; ---------------------------------------------------------------------------
;; Deck helpers
;; ---------------------------------------------------------------------------

(defn- shuffle-deck
  "Return a shuffled vector of 23 transaction types:
   10 :new-order, 10 :payment, 1 :delivery, 1 :order-status, 1 :stock-level."
  []
  (let [deck (ArrayList. 23)]
    (dotimes [_ 10] (.add deck :new-order))
    (dotimes [_ 10] (.add deck :payment))
    (.add deck :delivery)
    (.add deck :order-status)
    (.add deck :stock-level)
    (Collections/shuffle deck)
    (vec deck)))

;; ---------------------------------------------------------------------------
;; Latency and stats helpers (unchanged)
;; ---------------------------------------------------------------------------

(defn- record-latency! [^ConcurrentLinkedQueue queue ^long start-nanos]
  (let [elapsed (- (System/nanoTime) start-nanos)]
    (.add queue elapsed)))

(defn- drain-into! [^ConcurrentLinkedQueue queue ^MergingDigest digest]
  (loop []
    (when-let [v (.poll queue)]
      (.add digest (/ (double (long v)) 1e6))
      (recur))))

(defn latency-stats
  "Compute latency percentile stats from a TDigest. Returns nil if empty."
  [^MergingDigest digest]
  (let [n (.size digest)]
    (when (pos? n)
      {:n n
       :p50 (.quantile digest 0.50)
       :p90 (.quantile digest 0.90)
       :p95 (.quantile digest 0.95)
       :p99 (.quantile digest 0.99)
       :max (.getMax digest)})))

(defn- print-mix-stats [mix]
  (let [{:keys [no-remote-ol no-total-ol no-count no-rollback pay-count pay-remote pay-by-last os-count os-by-last]} mix]
    (println "  Mix validation:")
    (when (pos? no-count)
      (println (format "    NO rollback:     %.2f%% (spec: ~1%%)" (* 100.0 (/ (double no-rollback) no-count)))))
    (when (pos? no-total-ol)
      (println (format "    NO remote OL:    %.2f%% (spec: 0.95-1.05%%)" (* 100.0 (/ (double no-remote-ol) no-total-ol)))))
    (when (pos? pay-count)
      (println (format "    PAY remote:      %.2f%% (spec: 14-16%%)" (* 100.0 (/ (double pay-remote) pay-count))))
      (println (format "    PAY by-last:     %.2f%% (spec: 57-63%%)" (* 100.0 (/ (double pay-by-last) pay-count)))))
    (when (pos? os-count)
      (println (format "    OS by-last:      %.2f%% (spec: 57-63%%)" (* 100.0 (/ (double os-by-last) os-count)))))))

(defn- print-report [label result]
  (let [{:keys [elapsed-s total-count no-count tpm-c terminal-count mix latencies]} result]
    (println (format "\n%s %.0fs elapsed | %d terminals | %d total txns | %d new-orders | tpmC=%.0f"
                     label elapsed-s (or terminal-count 0) total-count no-count tpm-c))
    (doseq [[k lbl] [[:no-initiate "NO initiate"]
                     [:no-complete "NO complete"]
                     [:pay-initiate "PAY initiate"]
                     [:pay-complete "PAY complete"]
                     [:del-initiate "DEL initiate"]
                     [:order-status "Order-Status"]
                     [:stock-level "Stock-Level"]]]
      (when-let [stats (get latencies k)]
        (let [rate (double (/ (:n stats) elapsed-s))]
          (println (format "  %-20s n=%-6d rate=%.1f/s p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1fms"
                           lbl (:n stats) rate (:p50 stats) (:p90 stats) (:p95 stats) (:p99 stats) (:max stats))))))
    (print-mix-stats mix)))

;; ---------------------------------------------------------------------------
;; Terminal state
;; ---------------------------------------------------------------------------

(defn- make-terminal
  "Create a terminal for (w-id, d-id). Uses atoms for mutable deck state."
  [w-id d-id]
  {:w-id w-id :d-id d-id :deck (atom (shuffle-deck)) :deck-idx (atom 0)})

(defn- next-tx-type!
  "Pick next tx type from terminal's deck, advancing index and reshuffling as needed."
  [terminal]
  (let [deck-atom (:deck terminal)
        idx-atom (:deck-idx terminal)]
    (when (>= @idx-atom (count @deck-atom))
      (reset! deck-atom (shuffle-deck))
      (reset! idx-atom 0))
    (let [tx-type (nth @deck-atom @idx-atom)]
      (swap! idx-atom inc)
      tx-type)))

;; ---------------------------------------------------------------------------
;; Terminal event-driven cycle
;; ---------------------------------------------------------------------------

(declare start-terminal!)

(defn- schedule-delay!
  "Schedule a callback on the executor after delay-secs (scaled)."
  [^ScheduledThreadPoolExecutor scheduler callback ^double delay-secs ^double scale]
  (let [delay-ms (long (* delay-secs scale 1000.0))]
    (if (pos? delay-ms)
      (.schedule scheduler ^Runnable callback delay-ms TimeUnit/MILLISECONDS)
      (.submit scheduler ^Runnable callback))))

(defn- on-tx-complete!
  "Called when a transaction response arrives."
  [terminal ^ScheduledThreadPoolExecutor scheduler ctx tx-type]
  (let [{:keys [^AtomicBoolean running? ^AtomicInteger active-terminals
                time-scale mode]} ctx]
    (if (= mode :max-throughput)
      (.release ^Semaphore (:semaphore ctx))
      (if (.get running?)
        (let [mean (get THINK-TIME-MEANS tx-type 12.0)
              think-secs (exponential-think-time mean)]
          (schedule-delay! scheduler
                           (fn on-think-done []
                             (if (.get running?)
                               (start-terminal! terminal scheduler ctx)
                               (.decrementAndGet active-terminals)))
                           think-secs
                           time-scale))
        (.decrementAndGet active-terminals)))))

(defn- cached-warehouse-info
  "Return warehouse info for w-id, fetching from PState on cache miss."
  [load-ctx w-id]
  (let [^ConcurrentHashMap cache (:warehouse-info-cache load-ctx)]
    (or (.get cache w-id)
        (let [info (foreign-select-one (keypath w-id) (:warehouse-info-ps load-ctx))]
          (.putIfAbsent cache w-id info)
          info))))

(defn- cached-district-info
  "Return district info for [w-id d-id], fetching from PState on cache miss."
  [load-ctx w-id d-id]
  (let [^ConcurrentHashMap cache (:district-info-cache load-ctx)
        k [w-id d-id]]
    (or (.get cache k)
        (let [info (foreign-select-one (keypath k) (:district-info-ps load-ctx) {:pkey w-id})]
          (.putIfAbsent cache k info)
          info))))

(def ^:private ITEM-NOT-FOUND (Object.))

(defn- cached-item
  "Return item info for i-id, fetching from PState on cache miss.
   Returns nil for non-existent items (caches the miss to avoid repeated queries)."
  [load-ctx i-id]
  (let [^ConcurrentHashMap cache (:items-cache load-ctx)
        v (.get cache i-id)]
    (cond
      (identical? v ITEM-NOT-FOUND) nil
      (some? v) v
      :else (let [info (foreign-select-one (keypath i-id) (:items-ps load-ctx))]
              (.putIfAbsent cache i-id (or info ITEM-NOT-FOUND))
              info))))

(defn- submit-tx!
  "Generate and submit a transaction asynchronously."
  [terminal ^ScheduledThreadPoolExecutor scheduler ctx tx-type]
  (let [{:keys [tx-depot os-query sl-query
                result-ps pay-result-ps num-warehouses load-ctx
                no-initiate-q no-complete-q pay-initiate-q pay-complete-q
                del-initiate-q os-q sl-q
                no-count no-complete-count pay-complete-count total-count
                no-remote-ol no-total-ol pay-count pay-remote pay-by-last
                no-rollback os-count os-by-last
                ^long warmup-end-nanos]} ctx
        w-id (:w-id terminal)
        d-id (:d-id terminal)
        t0 (System/nanoTime)
        recording? (>= t0 warmup-end-nanos)
        on-complete (fn [tx-type-kw]
                      (on-tx-complete! terminal scheduler ctx tx-type-kw))]
    (when recording? (swap! total-count inc))
    (case tx-type
      :new-order
      (let [tx (if (:all-remote? ctx)
                 (dg/gen-new-order-tx-terminal-all-remote w-id d-id num-warehouses)
                 (dg/gen-new-order-tx-terminal w-id d-id num-warehouses))
            req-id (:request-id tx)
            day-bucket (h/request-id-day-bucket req-id)
            ols (:ols tx)]
        (when recording?
          (swap! no-count inc)
          (swap! no-total-ol + (count ols))
          (swap! no-remote-ol + (count (filter #(not= (:sup-wid %) w-id) ols))))
        ;; Ensure immutable data is cached for client-side display
        (cached-warehouse-info load-ctx w-id)
        (cached-district-info load-ctx w-id d-id)
        (doseq [ol ols] (cached-item load-ctx (:i-id ol)))
        (let [done? (volatile! false)
              proxy-state (atom nil) ;; nil → proxy or :close
              pf (foreign-proxy-async
                   (keypath day-bucket req-id) result-ps
                   {:pkey w-id
                    :callback-fn
                    (fn [new-val _diff _old]
                      (when (and new-val (not @done?))
                        (vreset! done? true)
                        (when recording?
                          (when (instance? NewOrderInvalidResult new-val)
                            (swap! no-rollback inc))
                          ;; Derive spec-required display fields client-side
                          (when-not (instance? NewOrderInvalidResult new-val)
                            (let [w-info (cached-warehouse-info load-ctx w-id)
                                  d-info (cached-district-info load-ctx w-id d-id)
                                  w-tax (:tax w-info)
                                  d-tax (:tax d-info)
                                  discount (:disc new-val)
                                  ol-amounts (mapv (fn [ol item-result]
                                                     (* (:price (cached-item load-ctx (:i-id ol)))
                                                        (double (:qty ol))))
                                                   ols (:items new-val))
                                  _total (* (reduce + 0.0 ol-amounts)
                                            (- 1.0 (double discount))
                                            (+ 1.0 (double w-tax) (double d-tax)))]))
                          (record-latency! no-complete-q t0)
                          (swap! no-complete-count inc))
                        (let [v (swap! proxy-state #(or % :close))]
                          (when-not (= v :close) (close! v)))
                        (on-complete :new-order)))})]
          (.thenAccept ^java.util.concurrent.CompletableFuture pf
            (reify Consumer
              (accept [_ ps]
                (let [v (swap! proxy-state #(or % ps))]
                  (when (= v :close) (close! ps))))))
          (.whenComplete
            (foreign-append-async! tx-depot tx :append-ack)
            (reify BiConsumer
              (accept [_ _ _] (when recording? (record-latency! no-initiate-q t0)))))))

      :payment
      (let [tx (dg/gen-payment-tx-terminal w-id d-id num-warehouses)
            req-id (:request-id tx)
            day-bucket (h/request-id-day-bucket req-id)]
        (when recording?
          (swap! pay-count inc)
          (when (not= (:c-w-id tx) w-id) (swap! pay-remote inc))
          (when (instance? PaymentByLastTx tx) (swap! pay-by-last inc)))
        ;; Ensure immutable data is cached for client-side display
        (cached-warehouse-info load-ctx w-id)
        (cached-district-info load-ctx w-id d-id)
        (let [done? (volatile! false)
              proxy-state (atom nil) ;; nil → proxy or :close
              pf (foreign-proxy-async
                   (keypath day-bucket req-id) pay-result-ps
                   {:pkey w-id
                    :callback-fn
                    (fn [new-val _diff _old]
                      (when (and new-val (not @done?))
                        (vreset! done? true)
                        (when recording?
                          (record-latency! pay-complete-q t0)
                          (swap! pay-complete-count inc))
                        (let [v (swap! proxy-state #(or % :close))]
                          (when-not (= v :close) (close! v)))
                        (on-complete :payment)))})]
          (.thenAccept ^java.util.concurrent.CompletableFuture pf
            (reify Consumer
              (accept [_ ps]
                (let [v (swap! proxy-state #(or % ps))]
                  (when (= v :close) (close! ps))))))
          (.whenComplete
            (foreign-append-async! tx-depot tx :append-ack)
            (reify BiConsumer
              (accept [_ _ _] (when recording? (record-latency! pay-initiate-q t0)))))))

      :delivery
      (let [tx (dg/gen-delivery-tx-terminal w-id)]
        (.whenComplete
          (foreign-append-async! tx-depot tx :append-ack)
          (reify BiConsumer
            (accept [_ _ _]
              (when recording? (record-latency! del-initiate-q t0))
              (on-complete :delivery)))))

      :order-status
      (let [by-last? (< (rand) 0.6)
            c-id (when-not by-last? (h/nurand 1023 1 dg/CUSTOMERS-PER-DIST dg/C-ID-RUN))
            c-last (when by-last? (dg/gen-last-name (h/nurand 255 0 (min 999 (dec dg/CUSTOMERS-PER-DIST)) dg/C-LAST-RUN)))]
        (when recording?
          (swap! os-count inc)
          (when by-last? (swap! os-by-last inc)))
        (.whenComplete
          (foreign-invoke-query-async os-query w-id d-id c-id c-last)
          (reify BiConsumer
            (accept [_ _ _]
              (when recording? (record-latency! os-q t0))
              (on-complete :order-status)))))

      :stock-level
      (let [threshold (+ 10 (rand-int 11))]
        (.whenComplete
          (foreign-invoke-query-async sl-query w-id d-id threshold)
          (reify BiConsumer
            (accept [_ _ _]
              (when recording? (record-latency! sl-q t0))
              (on-complete :stock-level))))))))

(defn- start-terminal!
  "Pick next tx type and begin the cycle.
   tpcc: schedule keying delay, then submit.
   max-throughput: acquire semaphore, submit, re-queue self."
  [terminal ^ScheduledThreadPoolExecutor scheduler ctx]
  (let [{:keys [^AtomicBoolean running? ^AtomicInteger active-terminals mode]} ctx]
    (if (= mode :max-throughput)
      (if (.get running?)
        (do
          (.acquire ^Semaphore (:semaphore ctx))
          (if (.get running?)
            (let [tx-type (next-tx-type! terminal)]
              (submit-tx! terminal scheduler ctx tx-type)
              (.submit scheduler ^Runnable (fn [] (start-terminal! terminal scheduler ctx))))
            (do
              (.release ^Semaphore (:semaphore ctx))
              (.decrementAndGet active-terminals))))
        (.decrementAndGet active-terminals))
      (let [{:keys [time-scale]} ctx
            tx-type (next-tx-type! terminal)
            keying-secs (get KEYING-TIMES tx-type 2.0)]
        (schedule-delay! scheduler
                         (fn on-keying-done []
                           (submit-tx! terminal scheduler ctx tx-type))
                         keying-secs
                         time-scale)))))

;; ---------------------------------------------------------------------------
;; Main entry point
;; ---------------------------------------------------------------------------

(defn make-load-context
  "Create reusable load runner context with cluster and all foreign clients.
   Accepts a cluster object or a conductor hostname string.
   Can be passed to run-load! / run-load-max! / run-load-print! / run-load-max-print!
   for multiple runs (e.g. a short warmup followed by a real measurement run)."
  ([cluster-or-host]
    (make-load-context cluster-or-host {}))
  ([cluster-or-host options]
    (let [cluster (if (string? cluster-or-host)
                    (open-cluster-manager-internal (merge options {"conductor.host" cluster-or-host}))
                    cluster-or-host)]
      {:cluster       cluster
       :tx-depot      (foreign-depot cluster module-name "*transaction-depot")
       :os-query      (foreign-query cluster module-name "order-status")
       :sl-query      (foreign-query cluster module-name "stock-level")
       :result-ps     (foreign-pstate cluster module-name "$$new-order-result")
       :pay-result-ps (foreign-pstate cluster module-name "$$payment-result")
       ;; Immutable data PState clients for client-side caching
       :warehouse-info-ps (foreign-pstate cluster module-name "$$warehouse-info")
       :district-info-ps  (foreign-pstate cluster module-name "$$district-info")
       :items-ps          (foreign-pstate cluster module-name "$$items")
       ;; Client-side caches for immutable data (populated lazily on first access)
       :warehouse-info-cache (ConcurrentHashMap.)
       :district-info-cache  (ConcurrentHashMap.)
       :items-cache          (ConcurrentHashMap.)})))

(defn- run-load-impl!
  "Core load driver. mode is :tpcc or :max-throughput."
  [load-ctx num-warehouses total-time-secs client-index total-clients
   {:keys [mode time-scale num-tickets all-remote? quiet?]}]
  (let [{:keys [tx-depot os-query sl-query result-ps pay-result-ps]} load-ctx
        ;; Latency queues + TDigest accumulators
        no-initiate-q (ConcurrentLinkedQueue.)  no-initiate-td (MergingDigest. 2500.0)
        pay-initiate-q (ConcurrentLinkedQueue.) pay-initiate-td (MergingDigest. 2500.0)
        del-initiate-q (ConcurrentLinkedQueue.) del-initiate-td (MergingDigest. 2500.0)
        no-complete-q (ConcurrentLinkedQueue.)  no-complete-td (MergingDigest. 2500.0)
        pay-complete-q (ConcurrentLinkedQueue.) pay-complete-td (MergingDigest. 2500.0)
        os-q (ConcurrentLinkedQueue.)           os-td (MergingDigest. 2500.0)
        sl-q (ConcurrentLinkedQueue.)           sl-td (MergingDigest. 2500.0)
        drain-all! (fn []
                     (drain-into! no-initiate-q no-initiate-td)
                     (drain-into! no-complete-q no-complete-td)
                     (drain-into! pay-initiate-q pay-initiate-td)
                     (drain-into! pay-complete-q pay-complete-td)
                     (drain-into! del-initiate-q del-initiate-td)
                     (drain-into! os-q os-td)
                     (drain-into! sl-q sl-td))
        ;; Counters
        no-count (atom 0)
        no-complete-count (atom 0)
        pay-complete-count (atom 0)
        total-count (atom 0)
        ;; Mix validation counters
        no-remote-ol (atom 0)
        no-total-ol (atom 0)
        pay-count (atom 0)
        pay-remote (atom 0)
        pay-by-last (atom 0)
        no-rollback (atom 0)
        os-count (atom 0)
        os-by-last (atom 0)
        ;; Running flag
        running? (AtomicBoolean. true)
        active-terminals (AtomicInteger. 0)
        ;; Timing
        start-time (System/currentTimeMillis)
        warmup-ms (if (> total-time-secs 180) 180000 0)
        warmup-end-ms (+ start-time warmup-ms)
        warmup-end-nanos (+ (System/nanoTime) (* warmup-ms 1000000))
        measurement-start-ms (long warmup-end-ms)
        report-interval-ms 10000
        build-mix (fn []
                    {:no-remote-ol @no-remote-ol :no-total-ol @no-total-ol
                     :no-count @no-count :no-rollback @no-rollback
                     :pay-count @pay-count :pay-remote @pay-remote :pay-by-last @pay-by-last
                     :os-count @os-count :os-by-last @os-by-last})
        build-latencies (fn []
                          {:no-initiate (latency-stats no-initiate-td)
                           :no-complete (latency-stats no-complete-td)
                           :pay-initiate (latency-stats pay-initiate-td)
                           :pay-complete (latency-stats pay-complete-td)
                           :del-initiate (latency-stats del-initiate-td)
                           :order-status (latency-stats os-td)
                           :stock-level (latency-stats sl-td)})
        ;; Compute terminal assignments for this client (partitioned by warehouse)
        wh-per-client (quot num-warehouses total-clients)
        extra (mod num-warehouses total-clients)
        my-start (+ (* client-index wh-per-client) (min client-index extra))
        my-count (+ wh-per-client (if (< client-index extra) 1 0))
        my-wh-start (inc my-start)
        my-wh-end (+ my-wh-start my-count)
        wh-ids (vec (range my-wh-start my-wh-end))
        my-terminals (vec (mapcat (fn [d-id]
                                    (map #(make-terminal % d-id) (shuffle wh-ids)))
                                  (range 1 (inc dg/DISTRICTS-PER-WH))))
        terminal-count (count my-terminals)
        build-result (fn []
                       (let [elapsed-s (/ (- (System/currentTimeMillis) measurement-start-ms) 1000.0)]
                         {:elapsed-s elapsed-s
                          :total-count @total-count
                          :no-count @no-count
                          :tpm-c (* 60.0 (/ @no-complete-count (max elapsed-s 0.001)))
                          :terminal-count terminal-count
                          :mix (build-mix)
                          :latencies (build-latencies)}))
        ;; Shared context map for terminals
        semaphore (when (= mode :max-throughput) (Semaphore. (int num-tickets)))
        ctx (merge
              {:tx-depot tx-depot
               :os-query os-query :sl-query sl-query
               :result-ps result-ps :pay-result-ps pay-result-ps
               :num-warehouses num-warehouses
               :no-initiate-q no-initiate-q :no-complete-q no-complete-q
               :pay-initiate-q pay-initiate-q :pay-complete-q pay-complete-q
               :del-initiate-q del-initiate-q :os-q os-q :sl-q sl-q
               :no-count no-count :no-complete-count no-complete-count
               :pay-complete-count pay-complete-count :total-count total-count
               :no-remote-ol no-remote-ol :no-total-ol no-total-ol
               :pay-count pay-count :pay-remote pay-remote :pay-by-last pay-by-last
               :no-rollback no-rollback :os-count os-count :os-by-last os-by-last
               :running? running? :active-terminals active-terminals
               :warmup-end-nanos warmup-end-nanos
               :mode mode
               :all-remote? all-remote?
               :load-ctx load-ctx}
              (if (= mode :max-throughput)
                {:semaphore semaphore}
                {:time-scale time-scale}))
        scheduler (ScheduledThreadPoolExecutor. 1)]
    ;; Print startup message
    (when-not quiet?
      (if (= mode :max-throughput)
        (println (format "Starting max-throughput load: %d terminals for %ds (%d warehouses, client %d/%d, num-tickets=%d, warmup=%ds)"
                         terminal-count total-time-secs num-warehouses
                         client-index total-clients (int num-tickets) (/ warmup-ms 1000)))
        (println (format "Starting load: %d terminals for %ds (%d warehouses, client %d/%d, time-scale=%.1fx, warmup=%ds)"
                         terminal-count total-time-secs num-warehouses
                         client-index total-clients time-scale (/ warmup-ms 1000)))))
    ;; Start all terminals
    (.set active-terminals terminal-count)
    (if (= mode :max-throughput)
      (doseq [terminal my-terminals]
        (.submit scheduler ^Runnable (fn [] (start-terminal! terminal scheduler ctx))))
      (doseq [terminal my-terminals]
        (start-terminal! terminal scheduler ctx)))
    ;; Main thread: sleep in 1s increments, report periodically
    (let [end-time (+ start-time (* total-time-secs 1000))]
      (let [warmup-announced? (atom (zero? warmup-ms))]
        (loop [last-report-ms warmup-end-ms]
          (let [now-ms (System/currentTimeMillis)]
            (when (< now-ms end-time)
              (Thread/sleep ^long (min 1000 (- end-time now-ms)))
              (let [now-ms (System/currentTimeMillis)
                    _ (when (and (not @warmup-announced?) (>= now-ms warmup-end-ms))
                        (when-not quiet?
                          (println (format "Warmup complete (%ds). Measurement started."
                                           (/ warmup-ms 1000))))
                        (reset! warmup-announced? true))
                    last-report-ms (if (and @warmup-announced?
                                            (>= (- now-ms last-report-ms) report-interval-ms))
                                     (do (drain-all!)
                                         (when-not quiet?
                                           (print-report "---" (build-result)))
                                         now-ms)
                                     last-report-ms)]
                (recur last-report-ms)))))))
    ;; Signal stop — snapshot results at measurement boundary
    (.set running? false)
    ;; For max-throughput, release extra permits to unblock any waiting terminal
    (when semaphore
      (.release semaphore terminal-count))
    (drain-all!)
    (let [result (build-result)]
      ;; Wait for active terminals to drain (finish current cycle)
      (let [deadline (+ (System/currentTimeMillis) 30000)]
        (loop []
          (when (and (pos? (.get active-terminals))
                     (< (System/currentTimeMillis) deadline))
            (Thread/sleep 100)
            (recur))))
      ;; Wait for proxy completions
      (let [deadline (+ (System/currentTimeMillis) 30000)]
        (loop []
          (when (and (or (< @no-complete-count @no-count)
                         (< @pay-complete-count @pay-count))
                     (< (System/currentTimeMillis) deadline))
            (Thread/sleep 100)
            (recur))))
      ;; Shutdown executor
      (.shutdown scheduler)
      (.awaitTermination scheduler 30 TimeUnit/SECONDS)
      result)))

(defn run-load!
  "Run TPC-C terminal-model load for total-time-secs.
   Takes a load context from make-load-context.
   Options:
     :time-scale  — multiplier for keying and think delays (default 1.0, 0.0 for tests)
     :all-remote? — if true, every new-order orderline goes to a remote warehouse
   Returns a map of stats."
  [load-ctx num-warehouses total-time-secs client-index total-clients
   & {:keys [time-scale all-remote? quiet?] :or {time-scale 1.0 all-remote? false quiet? false}}]
  (run-load-impl! load-ctx num-warehouses total-time-secs client-index total-clients
                   {:mode :tpcc :time-scale time-scale :all-remote? all-remote? :quiet? quiet?}))

(defn run-load-max!
  "Run TPC-C max-throughput load for total-time-secs.
   Takes a load context from make-load-context.
   Same terminal structure and tx mix as run-load!, but no keying/think delays.
   Uses a semaphore for flow control instead.
   num-tickets controls max concurrent in-flight transactions.
   Returns a map of stats."
  [load-ctx num-warehouses total-time-secs client-index total-clients num-tickets]
  (run-load-impl! load-ctx num-warehouses total-time-secs client-index total-clients
                  {:mode :max-throughput :num-tickets num-tickets}))

(defn- write-report-to-file [result label]
  (let [sw (java.io.StringWriter.)]
    (binding [*out* sw]
      (print-report label result))
    (let [report-str (str sw)]
      (spit "LOAD-RUNNER-RESULTS" report-str :append true)
      report-str)))

(defn run-load-print!
  "Run TPC-C load and print final report. Appends to LOAD-RUNNER-RESULTS file. Returns the stats map."
  [load-ctx num-warehouses total-time-secs client-index total-clients & opts]
  (let [result (apply run-load! load-ctx num-warehouses total-time-secs client-index total-clients opts)]
    (print-report "=== FINAL ===" result)
    (write-report-to-file result "=== FINAL ===")
    result))

(defn run-load-max-print!
  "Run TPC-C max-throughput load and print final report. Appends to LOAD-RUNNER-RESULTS file. Returns the stats map."
  [load-ctx num-warehouses total-time-secs client-index total-clients num-tickets]
  (let [result (run-load-max! load-ctx num-warehouses total-time-secs client-index total-clients num-tickets)]
    (print-report "=== FINAL ===" result)
    (write-report-to-file result "=== FINAL ===")
    result))
