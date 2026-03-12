(ns rpl.tpcc.seed
  "Client-side seed data generator. Runs as a standalone process,
   appending TPC-C initial data to TPCCModule depots via async appends
   with a semaphore to control concurrency."
  (:use [com.rpl.rama])
  (:require [rpl.tpcc.datagen :as dg])
  (:import [java.util.concurrent Semaphore]))

(def ^:private module-name "rpl.tpcc/TPCCModule")

(defn- client-range
  "Compute [start, end) for a client given total count."
  ^longs [^long total ^long index ^long total-clients]
  (let [start (quot (* total index) total-clients)
        end (quot (* total (inc index)) total-clients)]
    [start end]))

(defn- interleaved-index
  "Map a position to a flat index that cycles through warehouses.
   Original layout: [wh0-all-records, wh1-all-records, ...]
   Interleaved: wh0-rec0, wh1-rec0, wh2-rec0, ..., wh0-rec1, wh1-rec1, ..."
  ^long [^long pos ^long num-wh ^long rec-per-wh]
  (let [wh (mod pos num-wh)
        inner (quot pos num-wh)]
    (+ (* wh rec-per-wh) inner)))

(defn- append-phase!
  "Append records for one phase. gen-fn takes an index and returns a record."
  [^Semaphore sem depot gen-fn start end num-wh rec-per-wh]
  (loop [pos start]
    (when (< pos end)
      (.acquire sem 1)
      (let [idx (if rec-per-wh
                  (interleaved-index pos num-wh rec-per-wh)
                  pos)
            record (gen-fn idx)]
        (.whenComplete
          (foreign-append-async! depot record :append-ack)
          (reify java.util.function.BiConsumer
            (accept [_ _ _] (.release sem 1)))))
      (recur (inc pos)))))

(defn gen-seed!
  "Generate and append seed data for a subset of TPC-C warehouses.
   Runs sequentially through phases: items, warehouses, districts,
   customers (with embedded orders), stock, history."
  [cluster num-wh index total-clients max-tickets]
  (let [sem (Semaphore. max-tickets)
        start-time (System/currentTimeMillis)
        item-depot (foreign-depot cluster module-name "*item-depot")
        warehouse-depot (foreign-depot cluster module-name "*warehouse-depot")
        district-depot (foreign-depot cluster module-name "*district-depot")
        customer-depot (foreign-depot cluster module-name "*customer-depot")
        stock-depot (foreign-depot cluster module-name "*stock-depot")
        phases [["items"      dg/NUM-ITEMS              item-depot      #(dg/gen-item (inc %))            nil]
                ["warehouses" num-wh                     warehouse-depot #(dg/gen-warehouse (inc %))       nil]
                ["districts"  (* num-wh dg/DISTRICTS-PER-WH)            district-depot  dg/gen-district-at-index  dg/DISTRICTS-PER-WH]
                ["customers"  (* num-wh (dg/customer-batches-per-wh))   customer-depot  (let [perm (dg/make-customer-perm)] #(dg/gen-customer-at-index perm %))  (dg/customer-batches-per-wh)]
                ["stock"      (* num-wh (dg/stock-batches-per-wh))       stock-depot     dg/gen-stock-at-index     (dg/stock-batches-per-wh)]]]
    (doseq [[phase-name total depot gen-fn rec-per-wh] phases]
      (let [[start end] (client-range total index total-clients)
            cnt (- end start)]
        (println (str "Phase " phase-name ": " cnt " records [" start ", " end ")"))
        (append-phase! sem depot gen-fn start end num-wh rec-per-wh)
        (println (str "Phase " phase-name " appends sent."))))
    ;; Wait for all outstanding appends to complete
    (.acquire sem max-tickets)
    (.release sem max-tickets)
    (let [elapsed (- (System/currentTimeMillis) start-time)]
      (println (str "Seed complete in " (quot elapsed 1000) "s")))))

(defn run-seed! [conductor-host num-warehouses index total-clients max-tickets]
  (gen-seed! (open-cluster-manager-internal {"conductor.host" conductor-host})
             num-warehouses
             index
             total-clients
             max-tickets))
