(ns rpl.tpcc.datagen
  "TPC-C data generation functions. Pure Clojure — produces Load* record instances."
  (:require [rpl.tpcc.helpers :as h]
            [rpl.tpcc.types :refer [->LoadItem ->LoadWarehouse ->LoadDistrict
                                    ->LoadCustomer ->LoadStock
                                    ->OrderLineInput
                                    ->NewOrderTx ->PaymentByIdTx ->PaymentByLastTx ->DeliveryTx]])
  (:import [java.util ArrayList Collections Random]))

(def SYLLABLES ["BAR" "OUGHT" "ABLE" "PRI" "PRES" "ESE" "ANTI" "CALLY" "ATION" "EING"])

;; NURand C constants (Clause 2.1.6)
;; C-Load: fixed so all independent seed clients generate the same last names.
;; C-Run: |C-Load - C-Run| must be in [65..119] excluding 96, 112.
(def C-LAST-LOAD 0)
(def C-LAST-RUN 65)    ;; delta = 65, in [65..119]\{96,112}
(def C-ID-RUN 259)
(def C-OL-I-ID-RUN 7911)

(def NUM-ITEMS 100000)
(def DISTRICTS-PER-WH 10)
(def CUSTOMERS-PER-DIST 3000)
(def ORDERS-PER-DIST 3000)
(def NEW-ORDER-START 2101) ;; orders 2101-3000 go in NEW-ORDER table
(def ITEMS-PER-WH 100000)

(defn gen-last-name
  "Generate TPC-C last name from 3-digit number (0-999)."
  [n]
  (let [d0 (mod (quot n 100) 10)
        d1 (mod (quot n 10) 10)
        d2 (mod n 10)]
    (str (SYLLABLES d0) (SYLLABLES d1) (SYLLABLES d2))))

(defn- insert-original
  "Replace 8 chars at a random position with 'ORIGINAL'."
  [^String s]
  (let [pos (rand-int (max 1 (- (count s) 8)))]
    (str (subs s 0 pos) "ORIGINAL" (subs s (min (count s) (+ pos 8))))))

(defn- maybe-original
  "10% chance: insert 'ORIGINAL' into the string."
  [s]
  (if (< (rand) 0.1) (insert-original s) s))

(defn gen-item [id]
  (->LoadItem id
              (inc (rand-int 10000))
              (h/rand-astring 14 24)
              (+ 1.0 (* (rand) (Math/nextUp 99.0)))
              (maybe-original (h/rand-astring 26 50))))

(defn gen-warehouse [w-id]
  (->LoadWarehouse w-id
                   (h/rand-astring 6 10)
                   (h/rand-astring 10 20)
                   (h/rand-astring 10 20)
                   (h/rand-astring 10 20)
                   (h/rand-letter-string 2 2)
                   (h/rand-zip)
                   (* (rand) (Math/nextUp 0.2))
                   300000.0))

(defn gen-district [d-id w-id]
  (->LoadDistrict d-id w-id
                  (h/rand-astring 6 10)
                  (h/rand-astring 10 20)
                  (h/rand-astring 10 20)
                  (h/rand-astring 10 20)
                  (h/rand-letter-string 2 2)
                  (h/rand-zip)
                  (* (rand) (Math/nextUp 0.2))
                  30000.0
                  (inc ORDERS-PER-DIST)))

;; ---------------------------------------------------------------------------
;; Per-index generators (for load module chunked processing)
;; ---------------------------------------------------------------------------

(defn district-index-keys
  "Decompose flat 0-based index to [w-id d-id]."
  [idx]
  [(inc (quot idx DISTRICTS-PER-WH))
   (inc (mod idx DISTRICTS-PER-WH))])

(def CUSTOMER-BATCH-SIZE 10)
(defn customer-batches-per-wh [] (* DISTRICTS-PER-WH (long (Math/ceil (/ CUSTOMERS-PER-DIST CUSTOMER-BATCH-SIZE)))))

(def STOCK-BATCH-SIZE 10)
(defn stock-batches-per-wh [] (long (Math/ceil (/ ITEMS-PER-WH STOCK-BATCH-SIZE))))

(defn stock-index-keys
  "Decompose flat 0-based index to [w-id batch-start].
   batch-start is 0-based index of the first item in the batch."
  [idx]
  (let [batches (stock-batches-per-wh)]
    [(inc (quot idx batches))
     (* (mod idx batches) STOCK-BATCH-SIZE)]))


(defn gen-district-at-index
  "Generate a LoadDistrict from a flat 0-based index."
  [idx]
  (let [[w-id d-id] (district-index-keys idx)]
    (gen-district d-id w-id)))

(defn make-customer-perm
  "Random permutation of [1..CUSTOMERS-PER-DIST] for O_C_ID assignment.
   Spec says 'a random permutation' — shared across all districts.
   Returns a map of {c-id -> o-id}."
  []
  (let [rng (Random. 42)
        perm (ArrayList.)]
    (loop [i 1]
      (when (<= i CUSTOMERS-PER-DIST)
        (.add perm i)
        (recur (inc i))))
    (Collections/shuffle perm rng)
    (loop [i 0 m (transient {})]
      (if (< i CUSTOMERS-PER-DIST)
        (recur (inc i) (assoc! m (.get ^java.util.List perm i) (inc i)))
        (persistent! m)))))

(defn customer-batch-index-keys
  "Decompose flat 0-based batch index to [w-id d-id batch-idx].
   Batches cycle: d-id within w-id, batch-idx within d-id."
  [idx]
  (let [batches-per-dist (long (Math/ceil (/ CUSTOMERS-PER-DIST CUSTOMER-BATCH-SIZE)))
        batch-idx (mod idx batches-per-dist)
        rem (quot idx batches-per-dist)
        d-id (inc (mod rem DISTRICTS-PER-WH))
        w-id (inc (quot rem DISTRICTS-PER-WH))]
    [w-id d-id batch-idx]))

(defn gen-customer-at-index
  "Generate a LoadCustomer batch from a flat 0-based index.
   customer-perm is a map of {c-id -> o-id}.
   Each batch contains up to CUSTOMER-BATCH-SIZE customers for one (w-id, d-id)."
  [customer-perm idx]
  (let [[w-id d-id batch-idx] (customer-batch-index-keys idx)
        start-id (inc (* batch-idx CUSTOMER-BATCH-SIZE))
        amount (min CUSTOMER-BATCH-SIZE (- CUSTOMERS-PER-DIST (* batch-idx CUSTOMER-BATCH-SIZE)))
        o-ids (loop [i 0 v (transient [])]
                (if (< i amount)
                  (recur (inc i) (conj! v (customer-perm (+ start-id i))))
                  (persistent! v)))]
    (->LoadCustomer w-id d-id start-id o-ids)))

(defn gen-stock-at-index
  "Generate a LoadStock batch from a flat 0-based index.
   Each batch contains up to STOCK-BATCH-SIZE item IDs for one warehouse."
  [idx]
  (let [[w-id batch-start] (stock-index-keys idx)
        start-id (inc batch-start)
        amount (min STOCK-BATCH-SIZE (- ITEMS-PER-WH batch-start))]
    (->LoadStock w-id start-id amount)))



;; ---------------------------------------------------------------------------
;; Run-phase transaction generators
;; ---------------------------------------------------------------------------

(defn- rand-other-warehouse
  "Pick a random warehouse in [1..num-warehouses] excluding w-id.
   Returns w-id itself when num-warehouses=1 (no other warehouse exists)."
  [num-warehouses w-id]
  (if (<= num-warehouses 1)
    w-id
    (let [other (inc (rand-int (dec num-warehouses)))]
      (if (>= other w-id) (inc other) other))))

(defn gen-delivery-tx
  "Generate a DeliveryTx for the run phase."
  [num-warehouses]
  (let [w-id (inc (rand-int num-warehouses))
        carrier-id (inc (rand-int 10))]
    (->DeliveryTx w-id carrier-id (java.util.UUID/randomUUID))))

(defn- gen-new-order-tx-terminal*
  [w-id d-id num-warehouses all-remote?]
  (let [c-id (h/nurand 1023 1 CUSTOMERS-PER-DIST C-ID-RUN)
        ol-cnt (+ 5 (rand-int 11))
        invalid-last? (< (rand) 0.01)
        orderlines (loop [i 0 v (transient [])]
                     (if (< i ol-cnt)
                       (let [last-line? (= i (dec ol-cnt))
                             i-id (if (and invalid-last? last-line?)
                                    (inc NUM-ITEMS)
                                    (h/nurand 8191 1 NUM-ITEMS C-OL-I-ID-RUN))]
                         (recur (inc i)
                                (conj! v (->OrderLineInput
                                          i-id
                                          (if (or all-remote? (< (rand) 0.01))
                                            (rand-other-warehouse num-warehouses w-id)
                                            w-id)
                                          (inc (rand-int 10))))))
                       (persistent! v)))]
    (->NewOrderTx w-id d-id c-id orderlines (h/make-request-id))))

(defn gen-new-order-tx-terminal
  "Generate a NewOrderTx for a specific terminal's (w-id, d-id)."
  [w-id d-id num-warehouses]
  (gen-new-order-tx-terminal* w-id d-id num-warehouses false))

(defn gen-new-order-tx-terminal-all-remote
  "Generate a NewOrderTx where every orderline is from a remote warehouse."
  [w-id d-id num-warehouses]
  (gen-new-order-tx-terminal* w-id d-id num-warehouses true))

(defn gen-payment-tx-terminal
  "Generate a PaymentByIdTx or PaymentByLastTx for a specific terminal's (w-id, d-id)."
  [w-id d-id num-warehouses]
  (let [remote? (< (rand) 0.15)
        c-w-id (if remote?
                 (rand-other-warehouse num-warehouses w-id)
                 w-id)
        c-d-id (if remote? (inc (rand-int DISTRICTS-PER-WH)) d-id)
        by-last? (< (rand) 0.6)
        h-amount (+ 1.0 (* (rand) 4999.0))]
    (if by-last?
      (let [c-last (gen-last-name (h/nurand 255 0 (min 999 (dec CUSTOMERS-PER-DIST)) C-LAST-RUN))]
        (->PaymentByLastTx w-id d-id c-w-id c-d-id c-last h-amount (h/make-request-id)))
      (let [c-id (h/nurand 1023 1 CUSTOMERS-PER-DIST C-ID-RUN)]
        (->PaymentByIdTx w-id d-id c-w-id c-d-id c-id h-amount (h/make-request-id))))))

(defn gen-delivery-tx-terminal
  "Generate a DeliveryTx for a specific terminal's w-id."
  [w-id]
  (->DeliveryTx w-id (inc (rand-int 10)) (java.util.UUID/randomUUID)))
