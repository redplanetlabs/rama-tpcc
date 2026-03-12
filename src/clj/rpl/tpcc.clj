(ns rpl.tpcc
  "TPC-C benchmark implementation as a Rama module.

   Partitioned by warehouse ID. All districts, customers, orders, stock,
   and history for a warehouse are colocated on the same partition.

   Three write transaction types as microbatch source handlers:
   - NewOrder: creates orders with 5-15 line items, cross-partition for remote stock
   - Payment: updates customer balance and warehouse/district YTD
   - Delivery: batch processes oldest undelivered orders per district

   Item table is replicated on all tasks via |all during load, read-only during benchmark."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [rpl.tpcc.datagen :as dg]
            [rpl.tpcc.helpers :as h]
            [rpl.tpcc.pobjects :as po]
            [rpl.tpcc.types :as types])
  (:import [java.util.concurrent ConcurrentHashMap]
           [rpl.tpcc.types NewOrderTx PaymentByIdTx PaymentByLastTx DeliveryTx]))

(def ^:dynamic *test-mode* false)

(def ITEMS-CACHE (ConcurrentHashMap.))
(def WAREHOUSE-INFO-CACHE (ConcurrentHashMap.))
(def DISTRICT-INFO-CACHE (ConcurrentHashMap.))

(defn clear-caches! []
  (.clear ^ConcurrentHashMap ITEMS-CACHE)
  (.clear ^ConcurrentHashMap WAREHOUSE-INFO-CACHE)
  (.clear ^ConcurrentHashMap DISTRICT-INFO-CACHE))

;; ---------------------------------------------------------------------------
;; Helper functions
;; ---------------------------------------------------------------------------

(defn compute-new-stock-qty [s-quantity ol-quantity]
  "TPC-C stock update: if stock > qty+10, subtract qty; else subtract and add 91."
  (if (>= s-quantity (+ ol-quantity 10))
    (- s-quantity ol-quantity)
    (+ (- s-quantity ol-quantity) 91)))

(defn sum-orderline-amounts [orderlines]
  (reduce + 0.0 (map :amt orderlines)))

(defn set-orderline-delivery-dates [orderlines delivery-d]
  (mapv #(assoc % :del-d delivery-d) orderlines))

(defn brand-generic [item-data stock-data]
  (if (and (.contains ^String item-data "ORIGINAL")
           (.contains ^String stock-data "ORIGINAL"))
    "B"
    "G"))

(defn add-orderline-dist-info [orderlines]
  (mapv #(assoc % :dinfo (h/rand-letter-string 24 24)) orderlines))

(defn distinct-orderline-item-ids [orders]
  (vec (into #{} (comp (mapcat :ols) (map :i-id)) (vals orders))))

(defn find-customer-by-last-name [matching-customers]
  "Given sorted list of [first id] pairs, return the one at position ceil(n/2)-1."
  (when (seq matching-customers)
    (let [n (count matching-customers)
          idx (dec (long (Math/ceil (/ n 2.0))))]
      (nth matching-customers idx))))

(defn items-cache-get [k]
  (get ITEMS-CACHE k))

(defn items-cache-put! [k v]
  (h/map-put! ITEMS-CACHE k v))

(defn warehouse-info-cache-get [k]
  (get WAREHOUSE-INFO-CACHE k))

(defn warehouse-info-cache-put! [k v]
  (h/map-put! WAREHOUSE-INFO-CACHE k v))

(defn district-info-cache-get [k]
  (get DISTRICT-INFO-CACHE k))

(defn district-info-cache-put! [k v]
  (h/map-put! DISTRICT-INFO-CACHE k v))

;; ---------------------------------------------------------------------------
;; Partitioning
;; ---------------------------------------------------------------------------

(defdepotpartitioner warehouse-depot-partitioner [data num-partitions]
  (h/warehouse-partition num-partitions (:w-id data)))

(deframaop |warehouse [*w-id]
  (|custom h/warehouse-partition *w-id)
  (:>))

;; ---------------------------------------------------------------------------
;; Seed data loading ops
;; ---------------------------------------------------------------------------

(deframaop load-items>
  [{:keys [*id] :as *item}]
  (<<with-substitutions
   [$$items (po/items-task-global)]
   (|all)
   (local-transform> [(keypath *id)
                      (termval (dissoc *item :id))]
                     $$items)))

(deframaop load-warehouse>
  [{:keys [*w-id *ytd] :as *warehouse}]
  (<<with-substitutions
   [$$warehouse-ytd (po/warehouse-ytd-task-global)
    $$warehouse-info (po/warehouse-info-task-global)]
   (local-transform> [(keypath *w-id) (termval *ytd)] $$warehouse-ytd)
   (local-transform> [(keypath *w-id)
                      (termval (dissoc *warehouse :w-id :ytd))]
                     $$warehouse-info)))

(deframaop load-district>
  [{:keys [*id *w-id *ytd *next-oid] :as *district}]
  (<<with-substitutions
   [$$district (po/district-task-global)
    $$district-info (po/district-info-task-global)]
   (local-transform> [(keypath [*w-id *id])
                      (termval {:ytd *ytd :next-oid *next-oid})]
                     $$district)
   (local-transform> [(keypath [*w-id *id])
                      (termval (dissoc *district :id :w-id :ytd :next-oid))]
                     $$district-info)))

(defn gen-seed-order
  "Generate order fields for initial seed from o-id, w-id, and timestamp.
   Returns {:carrier :pending? :ols}."
  [^long o-id ^long w-id ^long now]
  (let [pending? (>= o-id dg/NEW-ORDER-START)
        carrier-id (when-not pending? (inc (h/fast-rand-int 10)))
        ol-cnt (+ 5 (h/fast-rand-int 11))
        delivery-d (when-not pending? now)
        orderlines (loop [i (int 0) v (transient [])]
                     (if (< i ol-cnt)
                       (recur (inc i)
                              (conj! v {:i-id        (inc (h/fast-rand-int dg/NUM-ITEMS))
                                        :sup-wid w-id
                                        :del-d  delivery-d
                                        :qty    5
                                        :amt      (if pending?
                                                       (+ 0.01 (* (h/fast-rand) 9999.98))
                                                       0.0)
                                        :dinfo   (h/rand-letter-string 24 24)}))
                       (persistent! v)))]
    {:carrier carrier-id :pending? pending? :ols orderlines}))

(deframaop load-order>
  [*w-id *d-id *o-id *c-id]
  (<<with-substitutions
   [$$order (po/order-task-global)
    $$neworder (po/neworder-task-global)
    $$customer (po/customer-task-global)]
   ;; Generate order data in the module
   (System/currentTimeMillis :> *now)
   (gen-seed-order *o-id *w-id *now :> {*carrier :carrier *pending? :pending? *ols :ols})
   ;; 1. Insert order
   (local-transform> [(keypath [*w-id *d-id *o-id])
                      (termval {:c-id *c-id
                                :entry-d *now
                                :carrier *carrier
                                :local? true
                                :ols *ols})]
                     $$order)
   ;; 2. Insert into neworder queue if pending
   (<<if *pending?
     (local-transform> [(keypath [*w-id *d-id]) NONE-ELEM (termval *o-id)] $$neworder))
   ;; 3. Update customer's last order
   (local-transform> [(keypath [*w-id *d-id *c-id] :last-oid) (termval *o-id)] $$customer)))

(defn gen-customer-filler
  "Generate all fields for a customer record from just the c-id.
   Last name is deterministic for c-id <= 1000, NURand for the rest.
   Credit is 10% BC, 90% GC. Other fields are per TPC-C spec defaults."
  [^long c-id]
  {:first        (h/rand-astring 8 16)
   :mid       "OE"
   :last         (if (<= c-id 1000)
                   (dg/gen-last-name (dec c-id))
                   (dg/gen-last-name (h/nurand 255 0 999 dg/C-LAST-LOAD)))
   :cr       (if (< (h/fast-rand) 0.1) "BC" "GC")
   :disc     (* (h/fast-rand) 0.5)
   :bal      -10.0
   :ytd-pay  10.0
   :pay-cnt  1
   :del-cnt 0
   :since        (System/currentTimeMillis)
   :cr-lim   50000.0
   :st1     (h/rand-astring 10 20)
   :st2     (h/rand-astring 10 20)
   :city         (h/rand-astring 10 20)
   :state        (h/rand-letter-string 2 2)
   :zip          (h/rand-zip)
   :phone        (h/rand-nstring 16 16)
   :data         (h/rand-astring 300 500)})

(defn gen-history-filler [w-id d-id]
  {:c-d-id d-id :c-w-id w-id :d-id d-id :w-id w-id
   :date (System/currentTimeMillis) :amt 10.0
   :data (h/rand-astring 12 24)})

(deframaop load-customer>
  [{:keys [*w-id *d-id *start-id *o-ids]}]
  (<<with-substitutions
   [$$customer (po/customer-task-global)
    $$customer-detail (po/customer-detail-task-global)
    $$customer-by-last (po/customer-by-last-task-global)
    $$history (po/history-task-global)]
   (ops/range> 0 (count *o-ids) :> *i)
   (+ *start-id *i :> *id)
   (nth *o-ids *i :> *o-id)
   (gen-customer-filler *id :> {:keys [*first *last] :as *filler})
   (select-keys *filler [:first :mid :last :cr :disc :bal :ytd-pay :pay-cnt :del-cnt] :> *cust-main)
   (select-keys *filler [:st1 :st2 :city :state :zip :phone :since :cr-lim :data] :> *cust-detail-data)
   (local-transform> [(keypath [*w-id *d-id *id])
                      (termval *cust-main)]
                     $$customer)
   (local-transform> [(keypath [*w-id *d-id *id])
                      (termval *cust-detail-data)]
                     $$customer-detail)
   (local-transform> [(keypath [*w-id *d-id *last])
                      NIL->VECTOR
                      (multi-path [AFTER-ELEM (termval [*first *id])]
                                  (term h/sortv))]
                     $$customer-by-last)
   ;; Generate and write initial history record
   (gen-history-filler *w-id *d-id :> *hist-filler)
   (assoc *hist-filler :c-id *id :> *hist)
   (h/random-uuid7 :> *history-key)
   (local-transform> [(keypath [*w-id *d-id] *history-key)
                      (termval *hist)]
                     $$history)
   ;; Generate and write order from o-id
   (load-order> *w-id *d-id *o-id *id)))

(defn gen-stock-detail-data
  "Generate random fields for a stock record (dist-info, data).
   These don't need to come from the client."
  []
  {:dinfo (loop [v (transient [])
                     i 0]
                (if (< i 10)
                  (recur (conj! v (h/rand-letter-string 24 24)) (inc i))
                  (persistent! v)
                  ))
   :data (let [s (h/rand-astring 26 50)]
           (if (< (h/fast-rand) 0.1)
             (let [pos (h/fast-rand-int (max 1 (- (count s) 8)))]
               (str (subs s 0 pos) "ORIGINAL" (subs s (min (count s) (+ pos 8)))))
             s))})

(deframaop load-stock>
  [{:keys [*w-id *start-id *amt]}]
  (<<with-substitutions
   [$$stock (po/stock-task-global)
    $$stock-detail (po/stock-detail-task-global)]
   (ops/range> *start-id (+ *start-id *amt) :> *i-id)
   (+ 10 (h/fast-rand-int 91) :> *quantity)
   (local-transform> [(keypath [*w-id *i-id])
                      (termval {:qty *quantity :ytd 0 :ord-cnt 0 :rem-cnt 0})]
                     $$stock)
   (gen-stock-detail-data :> *stock-detail-data)
   (local-transform> [(keypath [*w-id *i-id])
                      (termval *stock-detail-data)]
                     $$stock-detail)))


;; ---------------------------------------------------------------------------
;; Transaction ops
;; ---------------------------------------------------------------------------


(deframafn retrieve-item [*id]
  (<<with-substitutions
   [$$items (po/items-task-global)]
   (items-cache-get *id :> *v)
   (<<if (some? *v)
     (:> *v)
    (else>)
     (local-select> (keypath *id) $$items :> *v)
     (<<if (some? *v)
       (items-cache-put! *id *v))
     (:> *v))))

(deframafn retrieve-warehouse-info [*w-id]
  (<<with-substitutions
   [$$warehouse-info (po/warehouse-info-task-global)]
   (warehouse-info-cache-get *w-id :> *v)
   (<<if (some? *v)
     (:> *v)
    (else>)
     (local-select> (keypath *w-id) $$warehouse-info :> *v)
     (<<if (some? *v)
       (warehouse-info-cache-put! *w-id *v))
     (:> *v))))

(deframafn retrieve-district-info [*w-id *d-id]
  (<<with-substitutions
   [$$district-info (po/district-info-task-global)]
   (district-info-cache-get [*w-id *d-id] :> *v)
   (<<if (some? *v)
     (:> *v)
    (else>)
     (local-select> (keypath [*w-id *d-id]) $$district-info :> *v)
     (<<if (some? *v)
       (district-info-cache-put! [*w-id *d-id] *v))
     (:> *v))))

(deframaop new-order-tx>
  [{:keys [*w-id *d-id *c-id *ols *request-id]}]
  (<<with-substitutions
   [$$district (po/district-task-global)
    $$customer (po/customer-task-global)
    $$order (po/order-task-global)
    $$neworder (po/neworder-task-global)
    $$stock (po/stock-task-global)
    $$stock-detail (po/stock-detail-task-global)
    $$new-order-result (po/new-order-result-task-global)
    $$processed-items (po/processed-items-task-global)]

   (seq *ols :> *ols-seq)

   ;; 1. Validate item IDs
   (loop<- [*rem *ols-seq
            *valid? true
            :> *all-items-valid?]
     (<<if (or> (nil? *rem) (not *valid?))
       (:> *valid?)
      (else>)
       (get (first *rem) :i-id :> *check-id)
       (retrieve-item *check-id :> *item)
       (continue> (next *rem) (some? *item))))

   ;; 2. Read common fields (needed for both success and rollback output)
   (retrieve-warehouse-info *w-id :> {*w-tax :tax})
   (retrieve-district-info *w-id *d-id :> {*d-tax :tax})
   (local-select> (keypath [*w-id *d-id] :next-oid) $$district :> *o-id)
   (local-select> (keypath [*w-id *d-id *c-id]) $$customer
                  :> {*c-discount :disc *c-last :last *c-credit :cr})

   (<<if (not *all-items-valid?)
     ;; Invalid item: write result with O_ID, C_LAST, C_CREDIT (no other writes)
     (h/request-id-day-bucket *request-id :> *day-bucket)
     (types/->NewOrderInvalidResult *o-id *c-last *c-credit :> *no-result)
     (local-transform> [(keypath *day-bucket *request-id) (termval *no-result)]
                       $$new-order-result)
    (else>)
     ;; 3. Increment D_NEXT_O_ID
     (local-transform> [(keypath [*w-id *d-id] :next-oid) (term inc)] $$district)
     (vector *w-id *d-id *o-id :> *order-key)

     ;; 4. Check if all-local
     (loop<- [*rem-ol *ols-seq
              *loc? true
              :> *all-local?]
       (<<if (or> (nil? *rem-ol) (not *loc?))
         (:> *loc?)
        (else>)
         (get (first *rem-ol) :sup-wid :> *sw)
         (= *sw *w-id :> *same?)
         (continue> (next *rem-ol) *same?)))
     ;; 5. Process each line item
     (System/currentTimeMillis :> *now)
     (local-transform> [(keypath *order-key)
                        (termval {:rem-ol *ols-seq
                                  :new-total 0.0
                                  :processed []
                                  :c-id *c-id
                                  :now *now
                                  :all-local? *all-local?
                                  :request-id *request-id
                                  :c-discount *c-discount
                                  :c-last *c-last
                                  :c-credit *c-credit
                                  })]
       $$processed-items)
     (loop<- []
       (local-select> (keypath *order-key)
         $$processed-items
         :> {*rem-ol :rem-ol})
       (<<if (nil? *rem-ol)
         (:>)
        (else>)
         ;; Get line item inputs
         (first *rem-ol :> {*ol-i-id :i-id *ol-supply-w-id :sup-wid *ol-quantity :qty})

         (retrieve-item *ol-i-id :> {*item-price :price *item-name :name *item-data :data})

         ;; Compute order line amount
         (* (double *item-price) (double *ol-quantity) :> *ol-amount)

         ;; Stock read and update on supply warehouse partition
         (<<if (not= *ol-supply-w-id *w-id)
           (|warehouse *ol-supply-w-id))
         (identity *order-key :> [*w-id *d-id *o-id])
         (local-select> (keypath [*ol-supply-w-id *ol-i-id] :qty) $$stock :> *old-qty)
         (local-select> (keypath [*ol-supply-w-id *ol-i-id]) $$stock-detail
           :> {*stock-data :data *stock-dist-info :dinfo})
         (compute-new-stock-qty *old-qty *ol-quantity :> *new-qty)
         (nth *stock-dist-info (dec *d-id) :> *ol-dist-info)
         (brand-generic *item-data *stock-data :> *ol-brand)
         (= *ol-supply-w-id *w-id :> *is-local-stock?)
         (<<ramafn %add-ol-qty [*v] (:> (+ *v *ol-quantity)))
         (<<if *is-local-stock?
           (local-transform> [(keypath [*ol-supply-w-id *ol-i-id])
                              (multi-path [:qty (termval *new-qty)]
                                          [:ytd (term %add-ol-qty)]
                                          [:ord-cnt (term inc)])]
                             $$stock)
          (else>)
           (local-transform> [(keypath [*ol-supply-w-id *ol-i-id])
                              (multi-path [:qty (termval *new-qty)]
                                          [:ytd (term %add-ol-qty)]
                                          [(multi-path :ord-cnt :rem-cnt) (term inc)])]
                             $$stock))

         (<<if (not= *ol-supply-w-id *w-id)
           (|warehouse *w-id))
         (<<ramafn %add-total [*v]
           (:> (+ *v *ol-amount)))
         (local-transform> [(keypath *order-key)
                            (multi-path
                              [:rem-ol (term next)]
                              [:new-total (term %add-total)]
                              [:processed
                               NIL->VECTOR
                               AFTER-ELEM
                               (termval {:i-id *ol-i-id :sup-wid *ol-supply-w-id
                                         :qty *ol-quantity :amt *ol-amount
                                         :dinfo *ol-dist-info :stock-qty *new-qty
                                         :item-name *item-name :item-price *item-price
                                         :brand-generic *ol-brand})])]
            $$processed-items)
         (continue>)))

     (local-select> (keypath *order-key)
       $$processed-items
       :> {*final-total :new-total
           *final-processed :processed
           *c-id :c-id
           *now :now
           *all-local? :all-local?
           *request-id :request-id
           *c-discount :c-discount
           *c-last :c-last
           *c-credit :c-credit
           })
     (identity *order-key :> [*w-id *d-id *o-id])

     ;; 6. Derive order's orderlines and result items from processed data
     (<<ramafn %extract-orderlines [*m]
       (:> (select-keys *m [:i-id :sup-wid :qty :amt :dinfo])))
     (mapv %extract-orderlines *final-processed :> *final-orderlines)
     (<<ramafn %extract-items [*m]
       (:> (select-keys *m [:stock-qty :brand-generic])))
     (mapv %extract-items *final-processed :> *final-items)

     ;; 7. Insert ORDER with complete orderlines
     (local-transform> [(keypath [*w-id *d-id *o-id])
                        (termval {:c-id *c-id :entry-d *now
                                  :local? *all-local?
                                  :ols *final-orderlines})]
                       $$order)

     ;; 8. Insert NEW-ORDER
     (local-transform> [(keypath [*w-id *d-id]) NONE-ELEM (termval *o-id)] $$neworder)

     ;; 9. Update customer's latest order
     (local-transform> [(keypath [*w-id *d-id *c-id] :last-oid) (termval *o-id)] $$customer)

     ;; 10. Write result for client feedback
     (h/request-id-day-bucket *request-id :> *day-bucket)
     (types/->NewOrderResult *o-id *now *c-discount *c-last *c-credit *final-items :> *no-result)
     (local-transform> [(keypath *day-bucket *request-id) (termval *no-result)]
                       $$new-order-result))))


(deframaop payment-tx>
  [{:keys [*w-id *d-id *c-w-id *c-d-id *h-amt *request-id] :as *data}]
  (<<with-substitutions
   [$$warehouse-ytd (po/warehouse-ytd-task-global)
    $$district (po/district-task-global)
    $$customer (po/customer-task-global)
    $$customer-detail (po/customer-detail-task-global)
    $$customer-by-last (po/customer-by-last-task-global)
    $$history (po/history-task-global)
    $$payment-result (po/payment-result-task-global)]

   ;; 1. Read warehouse info from cache, update YTD
   (<<ramafn %add-h-amount [*v] (:> (+ *v *h-amt)))
   (<<ramafn %sub-h-amount [*v] (:> (- *v *h-amt)))
   (retrieve-warehouse-info *w-id :> {*w-name :name})
   (local-transform> [(keypath *w-id) (term %add-h-amount)] $$warehouse-ytd)

   ;; 2. Read district info from cache, update YTD
   (retrieve-district-info *w-id *d-id :> {*d-name :name})
   (local-transform> [(keypath [*w-id *d-id] :ytd) (term %add-h-amount)] $$district)

   ;; 3. Go to customer's warehouse (no-op if same)
   (|warehouse *c-w-id)

   ;; 4. Resolve customer ID (by last name or by ID)
   (<<if (instance? rpl.tpcc.types.PaymentByLastTx *data)
     (get *data :last :> *last)
     (local-select> (keypath [*c-w-id *c-d-id *last]) $$customer-by-last :> *matches)
     (find-customer-by-last-name *matches :> *found)
     (nth *found 1 :> *resolved-c-id)
    (else>)
     (get *data :c-id :> *resolved-c-id))

   ;; 5. Read customer fields, update balance/ytd/cnt
   (local-select> (keypath [*c-w-id *c-d-id *resolved-c-id]) $$customer
     :> {*cust-credit :cr *c-first :first *c-middle :mid *c-last :last
         *c-discount :disc *c-balance :bal})
   (- *c-balance *h-amt :> *new-balance)
   (local-transform> [(keypath [*c-w-id *c-d-id *resolved-c-id])
                      (multi-path [:bal (term %sub-h-amount)]
                                  [:ytd-pay (term %add-h-amount)]
                                  [:pay-cnt (term inc)])]
                     $$customer)

   ;; 6. Read customer detail fields
   (local-select> (keypath [*c-w-id *c-d-id *resolved-c-id]) $$customer-detail
     :> {*c-street-1 :st1 *c-street-2 :st2 *c-city :city
         *c-state :state *c-zip :zip *c-phone :phone *c-since :since
         *c-credit-lim :cr-lim *c-data :data})

   ;; 7. BC credit: update data, prepare display
   (<<if (= *cust-credit "BC")
     (str *resolved-c-id " " *c-d-id " " *c-w-id " " *d-id " " *w-id " " *h-amt " " *c-data :> *new-raw)
     (min (count *new-raw) 500 :> *trunc)
     (subs *new-raw 0 *trunc :> *new-c-data)
     (local-transform> [(keypath [*c-w-id *c-d-id *resolved-c-id] :data) (termval *new-c-data)]
                       $$customer-detail)
     (subs *new-c-data 0 (min (count *new-c-data) 200) :> *c-data-display)
    (else>)
     (identity nil :> *c-data-display))

   ;; 8. Return to home warehouse
   (|warehouse *w-id)

   ;; 9. Insert history record
   (System/currentTimeMillis :> *pay-now)
   (str *w-name "    " *d-name :> *h-data)
   (h/random-uuid7 :> *history-key)
   (local-transform> [(keypath [*w-id *d-id] *history-key)
                      (termval {:c-id *resolved-c-id :c-d-id *c-d-id :c-w-id *c-w-id
                                :d-id *d-id :w-id *w-id :date *pay-now
                                :amt *h-amt :data *h-data})]
                     $$history)


   ;; 10. Write payment result with spec-required output fields
   (types/->PaymentCustomerInfo *c-first *c-middle *c-last
     *c-street-1 *c-street-2 *c-city *c-state *c-zip
     *c-phone *c-since :> *cust-info)
   (types/->PaymentResult *resolved-c-id *cust-info *pay-now
     *cust-credit *c-credit-lim *c-discount *new-balance *c-data-display :> *pay-result)
   (h/request-id-day-bucket *request-id :> *day-bucket)
   (local-transform> [(keypath *day-bucket *request-id) (termval *pay-result)]
                     $$payment-result)))


(deframaop delivery-tx>
  [{:keys [*w-id *carrier *request-id]}]
  (<<with-substitutions
   [$$neworder (po/neworder-task-global)
    $$order (po/order-task-global)
    $$customer (po/customer-task-global)
    $$delivery-result (po/delivery-result-task-global)]

   ;; Process each district 1-10, accumulate {d-id -> o-id} result
   (loop<- [*del-d-id 1
            *result {}
            :> *final-result]
     (yield-if-overtime)
     (<<if (> *del-d-id 10)
       (:> *result)
      (else>)
       ;; 1. Find oldest undelivered order
       (local-select> [(keypath [*w-id *del-d-id]) (sorted-set-range-from-start 1) (view first)] $$neworder :> *del-o-id)
       (<<if (nil? *del-o-id)
         (identity *result :> *new-result)
        (else>)
         ;; 2. Delete from NEW-ORDER
         (local-transform> [(keypath [*w-id *del-d-id]) (set-elem *del-o-id) NONE>] $$neworder)

         ;; 3. Read order, set carrier and delivery dates
         (local-select> (keypath [*w-id *del-d-id *del-o-id]) $$order
                        :> {*del-c-id :c-id *del-orderlines :ols})
         (System/currentTimeMillis :> *del-now)
         (sum-orderline-amounts *del-orderlines :> *del-total)
         (<<ramafn %set-delivery [*ols] (:> (set-orderline-delivery-dates *ols *del-now)))
         (local-transform> [(keypath [*w-id *del-d-id *del-o-id])
                            (multi-path [:carrier (termval *carrier)]
                                        [:ols (term %set-delivery)])]
                           $$order)

         ;; 5. Update customer balance and delivery count
         (<<ramafn %add-del-total [*v] (:> (+ *v *del-total)))
         (local-transform> [(keypath [*w-id *del-d-id *del-c-id])
                            (multi-path [:bal (term %add-del-total)]
                                        [:del-cnt (term inc)])]
                           $$customer)

         (assoc *result *del-d-id *del-o-id :> *new-result))
       (continue> (inc *del-d-id) *new-result)))

   ;; Write delivery result
   (local-transform> [(keypath *request-id) (termval *final-result)] $$delivery-result)))


(deframaop cleanup> []
  (<<with-substitutions
   [$$new-order-result (po/new-order-result-task-global)
    $$payment-result (po/payment-result-task-global)]
   (|all)
   (- (h/request-id-day-bucket (h/make-request-id)) 1 :> *cutoff-day)
   (local-transform> [(sorted-map-range 0 *cutoff-day) (termval nil)]
     $$new-order-result)
   (local-transform> [(sorted-map-range 0 *cutoff-day) (termval nil)]
     $$payment-result)))

;; ---------------------------------------------------------------------------
;; Query topologies
;; ---------------------------------------------------------------------------

(defn declare-queries [topologies]
  ;; ORDER-STATUS (TPC-C 2.6): Returns status of customer's last order.
  (<<query-topology topologies "order-status"
    [*w-id *d-id *c-id *c-last-search :> *result]
    (|warehouse *w-id)

    ;; 1. Resolve customer
    (<<if (some? *c-last-search)
      (local-select> (keypath [*w-id *d-id *c-last-search]) $$customer-by-last :> *os-matches)
      (find-customer-by-last-name *os-matches :> *os-found)
      (nth *os-found 1 :> *os-c-id)
     (else>)
      (identity *c-id :> *os-c-id))

    ;; 2. Read customer
    (local-select> (keypath [*w-id *d-id *os-c-id]) $$customer
                    :> {*c-balance :bal *c-first :first *c-middle :mid
                        *c-last :last *os-last-oid :last-oid})

    ;; 3. Read order, strip dist-info (not in spec output)
    (local-select> (keypath [*w-id *d-id *os-last-oid]) $$order
                    :> {*os-entry-d :entry-d *os-carrier-id :carrier
                        *os-orderlines :ols})
    (<<ramafn %strip-dist-info [*ol] (:> (dissoc *ol :dinfo)))
    (mapv %strip-dist-info *os-orderlines :> *os-orderlines-clean)

     ;; 4. Return result
    (types/->OrderStatusResult *os-c-id *c-balance *c-first *c-middle *c-last
      *os-last-oid *os-entry-d *os-carrier-id *os-orderlines-clean :> *result)
    (|origin))


  ;; STOCK-LEVEL (TPC-C 2.8): Counts distinct items with stock below threshold
  ;; among the last 20 orders in a district.
  (<<query-topology topologies "stock-level"
    [*w-id *d-id *threshold :> *sl-count]
    (|warehouse *w-id)
    ;; 1. Read district's next order ID
    (local-select> (keypath [*w-id *d-id] :next-oid) $$district :> *sl-next-oid)

    ;; 2. Range query for last 20 orders, extract distinct item IDs
    (- *sl-next-oid 20 :> *sl-low-oid)
    (local-select> [(sorted-map-range [*w-id *d-id *sl-low-oid]
                                      [*w-id *d-id *sl-next-oid])]
                    $$order :> *sl-orders)
    (distinct-orderline-item-ids *sl-orders :> *sl-item-ids)

    ;; 3. Count items with stock below threshold
    (ops/explode *sl-item-ids :> *sl-i-id)
    (local-select> (keypath [*w-id *sl-i-id] :qty) $$stock :> *sl-s-qty)
    (filter> (< *sl-s-qty *threshold))
    (|origin)
    (aggs/+count :> *sl-count)))

(defn- shared-module-definition [setup topologies]
  ;; Transaction depot (combined)
  (declare-depot setup *transaction-depot   warehouse-depot-partitioner)

  (if *test-mode*
    (declare-depot setup *cleanup-tick :random {:global? true})
    (declare-tick-depot setup *cleanup-tick (* 60 60 1000)))

  (set-launch-module-dynamic-option! setup "depot.max.entries.per.partition" 500)

  (let [topo (microbatch-topology topologies "tpcc")]
    (declare-pstate* topo (symbol po/ITEMS-NAME)              po/ITEMS-SCHEMA)
    (let [wkp {:key-partitioner h/warehouse-partition}]
      (declare-pstate* topo (symbol po/WAREHOUSE-YTD-NAME)       po/WAREHOUSE-YTD-SCHEMA wkp)
      (declare-pstate* topo (symbol po/WAREHOUSE-INFO-NAME)     po/WAREHOUSE-INFO-SCHEMA wkp)
      (declare-pstate* topo (symbol po/DISTRICT-NAME)           po/DISTRICT-SCHEMA wkp)
      (declare-pstate* topo (symbol po/DISTRICT-INFO-NAME)      po/DISTRICT-INFO-SCHEMA wkp)
      (declare-pstate* topo (symbol po/CUSTOMER-NAME)           po/CUSTOMER-SCHEMA wkp)
      (declare-pstate* topo (symbol po/CUSTOMER-BY-LAST-NAME)   po/CUSTOMER-BY-LAST-SCHEMA wkp)
      (declare-pstate* topo (symbol po/HISTORY-NAME)            po/HISTORY-SCHEMA wkp)
      (declare-pstate* topo (symbol po/ORDER-NAME)              po/ORDER-SCHEMA wkp)
      (declare-pstate* topo (symbol po/NEWORDER-NAME)           po/NEWORDER-SCHEMA wkp)
      (declare-pstate* topo (symbol po/STOCK-NAME)              po/STOCK-SCHEMA wkp)
      (declare-pstate* topo (symbol po/STOCK-DETAIL-NAME)       po/STOCK-DETAIL-SCHEMA wkp)
      (declare-pstate* topo (symbol po/CUSTOMER-DETAIL-NAME)    po/CUSTOMER-DETAIL-SCHEMA wkp)
      (declare-pstate* topo (symbol po/NEW-ORDER-RESULT-NAME)   po/NEW-ORDER-RESULT-SCHEMA wkp)
      (declare-pstate* topo (symbol po/DELIVERY-RESULT-NAME)    po/DELIVERY-RESULT-SCHEMA wkp)
      (declare-pstate* topo (symbol po/PAYMENT-RESULT-NAME)    po/PAYMENT-RESULT-SCHEMA wkp))

    (<<sources topo
      ;; Transactions
      (source> *transaction-depot :> %mb)
      (<<batch
        (filter> false)
        (materialize> :> $$processed-items))
      (%mb :> *data)
      (<<subsource *data
        (case> NewOrderTx)
        (new-order-tx> *data)

        (case> PaymentByIdTx)
        (payment-tx> *data)

        (case> PaymentByLastTx)
        (payment-tx> *data)

        (case> DeliveryTx)
        (delivery-tx> *data))

      (source> *cleanup-tick :> %mb)
      (%mb)
      (cleanup>))

    (declare-queries topologies)
    ;; Return topo so caller can attach seed sources
    topo))

(defmodule TPCCModule [setup topologies]
  (shared-module-definition setup topologies))

(defmodule TPCCModuleWithSeed {:module-name "TPCCModule"} [setup topologies]
  ;; Seed depots - initial data population
  (declare-depot setup *item-depot        :random)
  (declare-depot setup *warehouse-depot   warehouse-depot-partitioner)
  (declare-depot setup *district-depot    warehouse-depot-partitioner)
  (declare-depot setup *customer-depot    warehouse-depot-partitioner)
  (declare-depot setup *stock-depot       warehouse-depot-partitioner)

  (let [topo (shared-module-definition setup topologies)]
    (<<sources topo
      (source> *item-depot :> %mb)
      (%mb :> *data)
      (load-items> *data)

      (source> *warehouse-depot :> %mb)
      (%mb :> *data)
      (load-warehouse> *data)

      (source> *district-depot :> %mb)
      (%mb :> *data)
      (load-district> *data)

      (source> *customer-depot :> %mb)
      (%mb :> *data)
      (load-customer> *data)

      (source> *stock-depot :> %mb)
      (%mb :> *data)
      (load-stock> *data))))
