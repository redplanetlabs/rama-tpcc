(ns rpl.tpcc.tpcc-test
  (:use [clojure.test]
        [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]
            [rpl.tpcc :as tpcc]
            [rpl.tpcc.datagen :as dg]
            [rpl.tpcc.helpers :as h]
            [rpl.tpcc.load-runner :as lr]
            [rpl.tpcc.seed :as seed]
            [rpl.tpcc.types :as types])
  (:import [com.rpl.rama.helpers TopologyUtils]
           [rpl.tpcc.types NewOrderResult NewOrderInvalidResult]))

(defmethod clojure.test/report :begin-test-var [m] (println "  testing:" (-> m :var meta :name)))

(use-fixtures :each (fn [f] (tpcc/clear-caches!) (f)))

(defn rand-tasks
  "Random power-of-2 task count from [2, 4, 8] for better partitioning coverage."
  []
  (rand-nth [2 4 8]))

;; ---------------------------------------------------------------------------
;; Test data constructors
;; ---------------------------------------------------------------------------

(defn make-test-items []
  [(types/->LoadItem 1 1 "Item1" 10.0 "dataORIGINALxyz")
   (types/->LoadItem 2 2 "Item2" 20.0 "plaindata")
   (types/->LoadItem 3 3 "Item3" 5.0  "hasORIGINALinside")
   (types/->LoadItem 4 4 "Item4" 15.0 "normaldata")
   (types/->LoadItem 5 5 "Item5" 12.0 "stockitem5")
   (types/->LoadItem 6 6 "Item6" 8.0  "stockitem6")
   (types/->LoadItem 7 7 "Item7" 25.0 "stockitem7")
   (types/->LoadItem 8 8 "Item8" 30.0 "stockitem8")])

(defn make-test-warehouses []
  [(types/->LoadWarehouse 1 "WH1" "st1" "st2" "city1" "CA" "123411111" 0.1 300000.0)
   (types/->LoadWarehouse 2 "WH2" "st1" "st2" "city2" "NY" "567811111" 0.08 300000.0)])

(defn make-test-districts []
  [(types/->LoadDistrict 1 1 "D1" "st1" "st2" "city" "CA" "111111111" 0.05 30000.0 3001)
   (types/->LoadDistrict 2 1 "D2" "st1" "st2" "city" "CA" "111111111" 0.06 30000.0 3001)
   (types/->LoadDistrict 3 1 "D3" "st1" "st2" "city" "CA" "111111111" 0.07 30000.0 3001)
   (types/->LoadDistrict 1 2 "D2W1" "st1" "st2" "city" "NY" "111111111" 0.04 30000.0 3001)
   (types/->LoadDistrict 2 2 "D2W2" "st1" "st2" "city" "NY" "111111111" 0.05 30000.0 3001)])

(defn make-test-customers []
  ;; Each LoadCustomer batch is [w-id d-id start-id o-ids].
  ;; Module generates all fields (last name, credit, etc.) and order data.
  ;; o-ids < 2101 = delivered, o-ids >= 2101 = pending (in neworder queue).
  ;; Last names are deterministic: c-id=1→BARBARBAR, c-id=2→BARBAROUGHT, etc.
  [;; WH1/D1: 5 customers (c-ids 1-5)
   (types/->LoadCustomer 1 1 1 [1 2101 2 2102 3])
   ;; WH1/D2: 3 customers (c-ids 1-3)
   (types/->LoadCustomer 1 2 1 [1 2 2101])
   ;; WH1/D3: 2 customers (c-ids 1-2)
   (types/->LoadCustomer 1 3 1 [1 2])
   ;; WH2/D1: 3 customers (c-ids 1-3)
   (types/->LoadCustomer 2 1 1 [1 2 2101])
   ;; WH2/D2: 2 customers (c-ids 1-2)
   (types/->LoadCustomer 2 2 1 [1 2101])])

(defn make-test-stock []
  ;; Stock quantity is generated randomly [10..100] by the module
  ;; Each LoadStock record contains a warehouse ID, start item ID, and count
  [(types/->LoadStock 1 1 8)
   (types/->LoadStock 2 1 8)])

(defn setup-base-data!
  "Appends all base dataset records. Returns total count of appends."
  [ipc module-name]
  (let [item-depot      (foreign-depot ipc module-name "*item-depot")
        warehouse-depot (foreign-depot ipc module-name "*warehouse-depot")
        district-depot  (foreign-depot ipc module-name "*district-depot")
        customer-depot  (foreign-depot ipc module-name "*customer-depot")
        stock-depot     (foreign-depot ipc module-name "*stock-depot")
        items           (make-test-items)
        warehouses      (make-test-warehouses)
        districts       (make-test-districts)
        customers       (make-test-customers)
        stock           (make-test-stock)]
    (doseq [r items]      (foreign-append! item-depot r))
    (doseq [r warehouses] (foreign-append! warehouse-depot r))
    (doseq [r districts]  (foreign-append! district-depot r))
    (doseq [r customers]  (foreign-append! customer-depot r))
    (doseq [r stock]      (foreign-append! stock-depot r))
    (+ (count items) (count warehouses) (count districts) (count customers)
       (count stock))))

(defn wait! [ipc module-name count]
  (rtest/wait-for-microbatch-processed-count ipc module-name "tpcc" count))

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest seed-data-test
  (with-redefs [tpcc/*test-mode* true]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [module-name  (get-module-name tpcc/TPCCModuleWithSeed)
            append-count (setup-base-data! ipc module-name)
            _            (wait! ipc module-name append-count)
            items-ps     (foreign-pstate ipc module-name "$$items")
            warehouse-ytd-ps  (foreign-pstate ipc module-name "$$warehouse-ytd")
            warehouse-info-ps (foreign-pstate ipc module-name "$$warehouse-info")
            district-ps       (foreign-pstate ipc module-name "$$district")
            district-info-ps  (foreign-pstate ipc module-name "$$district-info")
            customer-ps  (foreign-pstate ipc module-name "$$customer")
            cust-last-ps (foreign-pstate ipc module-name "$$customer-by-last")
            stock-ps     (foreign-pstate ipc module-name "$$stock")
            stock-detail-ps (foreign-pstate ipc module-name "$$stock-detail")
            cust-detail-ps  (foreign-pstate ipc module-name "$$customer-detail")
            order-ps     (foreign-pstate ipc module-name "$$order")
            neworder-ps  (foreign-pstate ipc module-name "$$neworder")
            history-ps   (foreign-pstate ipc module-name "$$history")]

        ;; $$items
        (is (= 10.0 (foreign-select-one (keypath 1 :price) items-ps)))
        (is (.contains ^String (foreign-select-one (keypath 1 :data) items-ps) "ORIGINAL"))
        (is (= 30.0 (foreign-select-one (keypath 8 :price) items-ps)))
        (is (= "Item2" (foreign-select-one (keypath 2 :name) items-ps)))

        ;; $$warehouse-info + $$warehouse-ytd
        (is (= "WH1" (foreign-select-one (keypath 1 :name) warehouse-info-ps)))
        (is (= 0.1 (foreign-select-one (keypath 1 :tax) warehouse-info-ps)))
        (is (= 300000.0 (foreign-select-one (keypath 1) warehouse-ytd-ps)))
        (is (= "WH2" (foreign-select-one (keypath 2 :name) warehouse-info-ps)))
        (is (= 0.08 (foreign-select-one (keypath 2 :tax) warehouse-info-ps)))

        ;; $$district + $$district-info — pkey is w-id
        (is (= 0.05 (foreign-select-one (keypath [1 1] :tax) district-info-ps {:pkey 1})))
        (is (= 30000.0 (foreign-select-one (keypath [1 1] :ytd) district-ps {:pkey 1})))
        (is (= 3001 (foreign-select-one (keypath [1 1] :next-oid) district-ps {:pkey 1})))
        (is (= 0.07 (foreign-select-one (keypath [1 3] :tax) district-info-ps {:pkey 1})))
        (is (= 0.04 (foreign-select-one (keypath [2 1] :tax) district-info-ps {:pkey 2})))
        (is (= 0.05 (foreign-select-one (keypath [2 2] :tax) district-info-ps {:pkey 2})))

        ;; $$customer — pkey is w-id
        ;; Credit is randomly generated (10% BC, 90% GC)
        (is (#{"BC" "GC"} (foreign-select-one (keypath [1 1 1] :cr) customer-ps {:pkey 1})))
        (is (= -10.0 (foreign-select-one (keypath [1 1 1] :bal) customer-ps {:pkey 1})))
        ;; Last name is deterministic from c-id: gen-last-name(0) = BARBARBAR
        (is (= "BARBARBAR" (foreign-select-one (keypath [1 1 1] :last) customer-ps {:pkey 1})))
        (is (#{"BC" "GC"} (foreign-select-one (keypath [1 2 1] :cr) customer-ps {:pkey 1})))
        (is (string? (foreign-select-one (keypath [2 1 1] :first) customer-ps {:pkey 2})))

        ;; $$customer-by-last — pkey is w-id
        ;; Only c-id=1 has BARBARBAR (each c-id gets a unique last name for small c-ids)
        (let [matches (foreign-select-one (keypath [1 1 "BARBARBAR"]) cust-last-ps {:pkey 1})]
          (is (= 1 (count matches)))
          (is (= #{1} (set (map second matches)))))

        ;; $$customer-detail — pkey is w-id
        (is (string? (foreign-select-one (keypath [1 1 1] :data) cust-detail-ps {:pkey 1})))
        (is (string? (foreign-select-one (keypath [2 1 1] :data) cust-detail-ps {:pkey 2})))

        ;; $$stock — pkey is w-id (quantities are random [10..100])
        (let [qty (foreign-select-one (keypath [1 1] :qty) stock-ps {:pkey 1})]
          (is (<= 10 qty 100)))
        (is (= 0 (foreign-select-one (keypath [1 1] :ytd) stock-ps {:pkey 1})))
        (is (= 0 (foreign-select-one (keypath [1 1] :ord-cnt) stock-ps {:pkey 1})))
        (let [qty (foreign-select-one (keypath [1 3] :qty) stock-ps {:pkey 1})]
          (is (<= 10 qty 100)))
        (let [qty (foreign-select-one (keypath [2 1] :qty) stock-ps {:pkey 2})]
          (is (<= 10 qty 100)))

        ;; $$stock-detail — pkey is w-id
        (is (= 10 (count (foreign-select-one (keypath [1 1] :dinfo) stock-detail-ps {:pkey 1}))))

        ;; $$order — pkey is w-id
        ;; Delivered order (o-id=1): carrier-id is random [1..10]
        (is (some? (foreign-select-one (keypath [1 1 1] :carrier) order-ps {:pkey 1})))
        ;; Pending order (o-id=2101): no carrier-id
        (is (nil? (foreign-select-one (keypath [1 1 2101] :carrier) order-ps {:pkey 1})))
        (is (= 2 (foreign-select-one (keypath [1 1 2101] :c-id) order-ps {:pkey 1})))
        ;; Pending order in WH1/D2
        (is (nil? (foreign-select-one (keypath [1 2 2101] :carrier) order-ps {:pkey 1})))
        ;; Order exists in WH2
        (is (some? (foreign-select-one (keypath [2 1 2101]) order-ps {:pkey 2})))

        ;; $$neworder — subindexed set, pending orders (o-id >= 2101) are in queue
        (is (true? (foreign-select-one [(keypath [1 1]) (view contains? 2101)] neworder-ps {:pkey 1})))
        (is (true? (foreign-select-one [(keypath [1 1]) (view contains? 2102)] neworder-ps {:pkey 1})))
        (is (false? (foreign-select-one [(keypath [1 1]) (view contains? 1)] neworder-ps {:pkey 1})))
        (is (true? (foreign-select-one [(keypath [1 2]) (view contains? 2101)] neworder-ps {:pkey 1})))
        (is (false? (foreign-select-one [(keypath [1 3]) (view contains? 1)] neworder-ps {:pkey 1})))
        (is (true? (foreign-select-one [(keypath [2 2]) (view contains? 2101)] neworder-ps {:pkey 2})))

        ;; $$customer :last-oid — pkey is w-id
        (is (= 2101 (foreign-select-one (keypath [1 1 2] :last-oid) customer-ps {:pkey 1})))
        (is (= 2101 (foreign-select-one (keypath [1 2 3] :last-oid) customer-ps {:pkey 1})))
        (is (= 1 (foreign-select-one (keypath [1 3 1] :last-oid) customer-ps {:pkey 1})))

        ;; $$history — subindexed vector, use (view count)
        (is (= 5 (foreign-select-one [(keypath [1 1]) (view count)] history-ps {:pkey 1})))
        (is (= 3 (foreign-select-one [(keypath [2 1]) (view count)] history-ps {:pkey 2})))))))

(deftest new-order-test
  (with-redefs [tpcc/*test-mode* true]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [module-name   (get-module-name tpcc/TPCCModuleWithSeed)
            base-count    (setup-base-data! ipc module-name)
            _             (wait! ipc module-name base-count)
            tx-depot      (foreign-depot ipc module-name "*transaction-depot")
            district-ps   (foreign-pstate ipc module-name "$$district")
            customer-ps   (foreign-pstate ipc module-name "$$customer")
            order-ps      (foreign-pstate ipc module-name "$$order")
            neworder-ps   (foreign-pstate ipc module-name "$$neworder")
            stock-ps      (foreign-pstate ipc module-name "$$stock")
            result-ps     (foreign-pstate ipc module-name "$$new-order-result")
            mb-count      (atom base-count)
            ;; Read initial stock quantities (randomly generated by module)
            init-s1       (foreign-select-one (keypath [1 1] :qty) stock-ps {:pkey 1})
            init-s2       (foreign-select-one (keypath [1 2] :qty) stock-ps {:pkey 1})
            init-s4       (foreign-select-one (keypath [1 4] :qty) stock-ps {:pkey 1})
            init-s5       (foreign-select-one (keypath [1 5] :qty) stock-ps {:pkey 1})
            init-s5-wh2   (foreign-select-one (keypath [2 5] :qty) stock-ps {:pkey 2})
            ;; Read customer 2's discount and credit (randomly generated)
            c2-discount   (foreign-select-one (keypath [1 1 2] :disc) customer-ps {:pkey 1})
            c2-credit     (foreign-select-one (keypath [1 1 2] :cr) customer-ps {:pkey 1})
            c4-credit     (foreign-select-one (keypath [1 1 4] :cr) customer-ps {:pkey 1})]

        ;; === Success case: home warehouse ===
        (let [req-id (h/make-request-id)
              expected-s1 (tpcc/compute-new-stock-qty init-s1 3)
              expected-s2 (tpcc/compute-new-stock-qty init-s2 5)]
          (foreign-append! tx-depot
                           (types/->NewOrderTx 1 1 2
                                               [(types/->OrderLineInput 1 1 3)
                                                (types/->OrderLineInput 2 1 5)]
                                               req-id))
          (swap! mb-count inc)
          (wait! ipc module-name @mb-count)

          ;; District next-o-id incremented
          (is (= 3002 (foreign-select-one (keypath [1 1] :next-oid) district-ps {:pkey 1})))

          ;; Order created
          (let [order (foreign-select-one (keypath [1 1 3001]) order-ps {:pkey 1})]
            (is (= 2 (:c-id order)))
            (is (nil? (:carrier order)))
            (is (true? (:local? order)))
            (is (= 2 (count (:ols order))))
            ;; OL amounts: item1_price*3=30.0, item2_price*5=100.0
            (is (= 30.0 (:amt (first (:ols order)))))
            (is (= 100.0 (:amt (second (:ols order)))))
            (is (string? (:dinfo (first (:ols order)))))
            (is (string? (:dinfo (second (:ols order)))))
            (is (nil? (:del-d (first (:ols order)))))
            (is (nil? (:del-d (second (:ols order))))))

          ;; Neworder contains 3001
          (is (true? (foreign-select-one [(keypath [1 1]) (view contains? 3001)] neworder-ps {:pkey 1})))

          ;; Customer last-o-id updated
          (is (= 3001 (foreign-select-one (keypath [1 1 2] :last-oid) customer-ps {:pkey 1})))

          ;; Stock updates
          (is (= expected-s1 (foreign-select-one (keypath [1 1] :qty) stock-ps {:pkey 1})))
          (is (= 3 (foreign-select-one (keypath [1 1] :ytd) stock-ps {:pkey 1})))
          (is (= 1 (foreign-select-one (keypath [1 1] :ord-cnt) stock-ps {:pkey 1})))
          (is (= 0 (foreign-select-one (keypath [1 1] :rem-cnt) stock-ps {:pkey 1})))
          (is (= expected-s2 (foreign-select-one (keypath [1 2] :qty) stock-ps {:pkey 1})))
          (is (= 5 (foreign-select-one (keypath [1 2] :ytd) stock-ps {:pkey 1})))
          (is (= 1 (foreign-select-one (keypath [1 2] :ord-cnt) stock-ps {:pkey 1})))

          ;; Result
          (let [res (foreign-select-one (keypath (h/request-id-day-bucket req-id) req-id) result-ps {:pkey 1})]
            (is (instance? NewOrderResult res))
            (is (= 3001 (:o-id res)))
            (is (some? (:entry-d res)))
            (is (= c2-discount (:disc res)))
            ;; c-id=2 → gen-last-name(1) = BARBAROUGHT
            (is (= "BARBAROUGHT" (:last res)))
            (is (= c2-credit (:cr res)))
            ;; Brand-generic: "B" if both item and stock data contain ORIGINAL, else "G"
            (is (#{"B" "G"} (:brand-generic (first (:items res)))))
            (is (= "G" (:brand-generic (second (:items res)))))
            ;; Verify result items content (only non-input, non-derivable fields)
            (is (= expected-s1 (:stock-qty (first (:items res)))))
            (is (= expected-s2 (:stock-qty (second (:items res)))))))

        ;; === Invalid item rollback ===
        (let [req-id2 (h/make-request-id)
              stock-before (foreign-select-one (keypath [1 1] :qty) stock-ps {:pkey 1})]
          (foreign-append! tx-depot
                           (types/->NewOrderTx 1 1 4
                                               [(types/->OrderLineInput 1 1 1)
                                                (types/->OrderLineInput 999999 1 1)]
                                               req-id2))
          (swap! mb-count inc)
          (wait! ipc module-name @mb-count)

          ;; Result shows invalid
          (let [res (foreign-select-one (keypath (h/request-id-day-bucket req-id2) req-id2) result-ps {:pkey 1})]
            (is (instance? NewOrderInvalidResult res))
            (is (= 3002 (:o-id res)))
            ;; c-id=4 → gen-last-name(3) = BARBARPRI
            (is (= "BARBARPRI" (:last res)))
            (is (= c4-credit (:cr res))))

          ;; next-o-id NOT incremented (still 3002)
          (is (= 3002 (foreign-select-one (keypath [1 1] :next-oid) district-ps {:pkey 1})))
          ;; Order NOT created
          (is (nil? (foreign-select-one (keypath [1 1 3002]) order-ps {:pkey 1})))
          ;; Neworder does NOT contain 3002
          (is (false? (foreign-select-one [(keypath [1 1]) (view contains? 3002)] neworder-ps {:pkey 1})))
          ;; Stock unchanged
          (is (= stock-before (foreign-select-one (keypath [1 1] :qty) stock-ps {:pkey 1}))))

        ;; === Remote warehouse ===
        (let [req-id3 (h/make-request-id)
              expected-s4 (tpcc/compute-new-stock-qty init-s4 2)
              expected-s5-wh2 (tpcc/compute-new-stock-qty init-s5-wh2 3)]
          (foreign-append! tx-depot
                           (types/->NewOrderTx 1 2 1
                                               [(types/->OrderLineInput 4 1 2)    ;; home supply
                                                (types/->OrderLineInput 5 2 3)]   ;; remote supply from WH2
                                               req-id3))
          (swap! mb-count inc)
          (wait! ipc module-name @mb-count)

          ;; Order is not all-local
          (is (false? (foreign-select-one (keypath [1 2 3001] :local?) order-ps {:pkey 1})))

          ;; Remote stock: WH2 item 5
          (is (= expected-s5-wh2 (foreign-select-one (keypath [2 5] :qty) stock-ps {:pkey 2})))
          (is (= 1 (foreign-select-one (keypath [2 5] :rem-cnt) stock-ps {:pkey 2})))

          ;; Home stock: WH1 item 4
          (is (= expected-s4 (foreign-select-one (keypath [1 4] :qty) stock-ps {:pkey 1})))
          (is (= 0 (foreign-select-one (keypath [1 4] :rem-cnt) stock-ps {:pkey 1})))

          ;; WH1 item 5 unchanged (supplied from WH2)
          (is (= init-s5 (foreign-select-one (keypath [1 5] :qty) stock-ps {:pkey 1}))))

        ;; === Stock +91 rule + Duplicate item (batched in one microbatch) ===
        (let [req-id4 (h/make-request-id)
              req-id5 (h/make-request-id)
              ;; Compute expected stock changes based on actual initial values
              expected-s5-after (tpcc/compute-new-stock-qty init-s5 20)
              s1-after-success (tpcc/compute-new-stock-qty init-s1 3)
              s1-after-dup1 (tpcc/compute-new-stock-qty s1-after-success 2)
              s1-after-dup2 (tpcc/compute-new-stock-qty s1-after-dup1 4)]
          (rtest/pause-microbatch-topology! ipc module-name "tpcc")
          (foreign-append! tx-depot
                           (types/->NewOrderTx 1 1 5
                                               [(types/->OrderLineInput 5 1 20)]
                                               req-id4))
          (swap! mb-count inc)
          ;; Two lines both reference item 1; stock updated twice
          (foreign-append! tx-depot
                           (types/->NewOrderTx 1 1 3
                                               [(types/->OrderLineInput 1 1 2)
                                                (types/->OrderLineInput 1 1 4)]
                                               req-id5))
          (swap! mb-count inc)
          (rtest/resume-microbatch-topology! ipc module-name "tpcc")
          (wait! ipc module-name @mb-count)

          ;; Stock +91 / normal rule applied correctly
          (is (= expected-s5-after (foreign-select-one (keypath [1 5] :qty) stock-ps {:pkey 1})))
          ;; Duplicate: item 1 WH1 stock through two decreases after success case
          (is (= s1-after-dup2 (foreign-select-one (keypath [1 1] :qty) stock-ps {:pkey 1})))
          ;; ytd: was 3 from success, +2+4=9
          (is (= 9 (foreign-select-one (keypath [1 1] :ytd) stock-ps {:pkey 1})))
          ;; order-cnt: was 1 from success, +1+1=3
          (is (= 3 (foreign-select-one (keypath [1 1] :ord-cnt) stock-ps {:pkey 1}))))))))

(deftest payment-test
  (with-redefs [tpcc/*test-mode* true]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [module-name      (get-module-name tpcc/TPCCModuleWithSeed)
            base-count       (setup-base-data! ipc module-name)
            _                (wait! ipc module-name base-count)
            tx-depot        (foreign-depot ipc module-name "*transaction-depot")
            warehouse-ytd-ps (foreign-pstate ipc module-name "$$warehouse-ytd")
            district-ps      (foreign-pstate ipc module-name "$$district")
            customer-ps      (foreign-pstate ipc module-name "$$customer")
            cust-detail-ps   (foreign-pstate ipc module-name "$$customer-detail")
            history-ps       (foreign-pstate ipc module-name "$$history")
            cust-by-last-ps  (foreign-pstate ipc module-name "$$customer-by-last")
            mb-count         (atom base-count)
            ;; Read generated credit for c-id=1 (needed for BC test)
            c1-credit      (foreign-select-one (keypath [1 1 1] :cr) customer-ps {:pkey 1})]

        ;; === Payment by ID (home warehouse) ===
        (foreign-append! tx-depot
                         (types/->PaymentByIdTx 1 1 1 1 2 100.0 (h/make-request-id)))
        (swap! mb-count inc)
        (wait! ipc module-name @mb-count)

        (is (= 300100.0 (foreign-select-one (keypath 1) warehouse-ytd-ps)))
        (is (= 30100.0 (foreign-select-one (keypath [1 1] :ytd) district-ps {:pkey 1})))
        (is (= -110.0 (foreign-select-one (keypath [1 1 2] :bal) customer-ps {:pkey 1})))
        (is (= 110.0 (foreign-select-one (keypath [1 1 2] :ytd-pay) customer-ps {:pkey 1})))
        (is (= 2 (foreign-select-one (keypath [1 1 2] :pay-cnt) customer-ps {:pkey 1})))
        (is (string? (foreign-select-one (keypath [1 1 2] :data) cust-detail-ps {:pkey 1})))
        ;; History: new entry with data="WH1    D1" (4 spaces)
        (is (= 6 (foreign-select-one [(keypath [1 1]) (view count)] history-ps {:pkey 1})))
        (let [last-hist (foreign-select-one [(keypath [1 1]) (view last) LAST] history-ps {:pkey 1})]
          (is (= 2 (:c-id last-hist)))
          (is (= 1 (:c-d-id last-hist)))
          (is (= 1 (:c-w-id last-hist)))
          (is (= 100.0 (:amt last-hist)))
          (is (some? (:date last-hist)))
          (is (= "WH1    D1" (:data last-hist))))

        ;; === Payment by last name ===
        ;; BARBARBAR in WH1/D1: only c-id=1 matches (unique last names with small c-ids)
        (foreign-append! tx-depot
                         (types/->PaymentByLastTx 1 1 1 1 "BARBARBAR" 50.0 (h/make-request-id)))
        (swap! mb-count inc)
        (wait! ipc module-name @mb-count)

        ;; c-id=1 selected: balance -10 - 50 = -60
        (is (= -60.0 (foreign-select-one (keypath [1 1 1] :bal) customer-ps {:pkey 1})))
        ;; c-id=2: still -110 from by-ID payment
        (is (= -110.0 (foreign-select-one (keypath [1 1 2] :bal) customer-ps {:pkey 1})))
        ;; c-id=3: untouched
        (is (= -10.0 (foreign-select-one (keypath [1 1 3] :bal) customer-ps {:pkey 1})))

        ;; === BC/GC credit customer (C_DATA update) ===
        ;; Pay c-id=1 by ID — if BC, data gets prepended
        (foreign-append! tx-depot
                         (types/->PaymentByIdTx 1 1 1 1 1 50.0 (h/make-request-id)))
        (swap! mb-count inc)
        (wait! ipc module-name @mb-count)

        ;; c-id=1: balance = -60 - 50 = -110
        (is (= -110.0 (foreign-select-one (keypath [1 1 1] :bal) customer-ps {:pkey 1})))
        (is (= 110.0 (foreign-select-one (keypath [1 1 1] :ytd-pay) customer-ps {:pkey 1})))
        (is (= 3 (foreign-select-one (keypath [1 1 1] :pay-cnt) customer-ps {:pkey 1})))
        ;; BC credit: data prepended. GC: data unchanged but still a string
        (let [cdata (foreign-select-one (keypath [1 1 1] :data) cust-detail-ps {:pkey 1})]
          (if (= "BC" c1-credit)
            (do (is (.startsWith ^String cdata "1 1 1 1 1 50.0 "))
                (is (<= (count cdata) 500)))
            (is (string? cdata))))

        ;; === Cross-warehouse payment ===
        ;; Customer on WH2/D1 paying at WH1/D1
        (foreign-append! tx-depot
                         (types/->PaymentByIdTx 1 1 2 1 1 75.0 (h/make-request-id)))
        (swap! mb-count inc)
        (wait! ipc module-name @mb-count)

        ;; WH1 ytd: 300000 + 100 + 50 + 50 + 75 = 300275
        (is (= 300275.0 (foreign-select-one (keypath 1) warehouse-ytd-ps)))
        (is (= 30275.0 (foreign-select-one (keypath [1 1] :ytd) district-ps {:pkey 1})))
        ;; Customer on WH2: balance decreased, payment-cnt incremented
        (is (= -85.0 (foreign-select-one (keypath [2 1 1] :bal) customer-ps {:pkey 2})))
        (is (= 2 (foreign-select-one (keypath [2 1 1] :pay-cnt) customer-ps {:pkey 2})))

        ;; === By-last-name + Cross-warehouse by last name (batched in one microbatch) ===
        (rtest/pause-microbatch-topology! ipc module-name "tpcc")
        ;; BARBARBAR in WH1/D2: only c-id=1 matches
        (foreign-append! tx-depot
                         (types/->PaymentByLastTx 1 2 1 2 "BARBARBAR" 30.0 (h/make-request-id)))
        (swap! mb-count inc)
        ;; BARBARBAR in WH2/D1: only c-id=1 matches
        (foreign-append! tx-depot
                         (types/->PaymentByLastTx 1 1 2 1 "BARBARBAR" 25.0 (h/make-request-id)))
        (swap! mb-count inc)
        (rtest/resume-microbatch-topology! ipc module-name "tpcc")
        (wait! ipc module-name @mb-count)

        ;; WH1/D2/C1 selected: -10 - 30 = -40
        (is (= -40.0 (foreign-select-one (keypath [1 2 1] :bal) customer-ps {:pkey 1})))
        ;; WH1/D2/C2 untouched
        (is (= -10.0 (foreign-select-one (keypath [1 2 2] :bal) customer-ps {:pkey 1})))
        ;; Cross-wh by last name: WH2/D1/C1 was -85, now -110
        (is (= -110.0 (foreign-select-one (keypath [2 1 1] :bal) customer-ps {:pkey 2})))
        (is (= 3 (foreign-select-one (keypath [2 1 1] :pay-cnt) customer-ps {:pkey 2})))))))

(deftest delivery-test
  (with-redefs [tpcc/*test-mode* true]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [module-name   (get-module-name tpcc/TPCCModuleWithSeed)
            base-count    (setup-base-data! ipc module-name)
            _             (wait! ipc module-name base-count)
            tx-depot     (foreign-depot ipc module-name "*transaction-depot")
            order-ps      (foreign-pstate ipc module-name "$$order")
            neworder-ps   (foreign-pstate ipc module-name "$$neworder")
            customer-ps   (foreign-pstate ipc module-name "$$customer")
            del-result-ps (foreign-pstate ipc module-name "$$delivery-result")
            mb-count      (atom base-count)]

        ;; Create an additional pending order in WH1/D1 to test oldest-first delivery
        (foreign-append! tx-depot
                         (types/->NewOrderTx 1 1 1
                                             [(types/->OrderLineInput 1 1 2)]
                                             (h/make-request-id)))
        (swap! mb-count inc)
        (wait! ipc module-name @mb-count)
        ;; WH1/D1 now has 3 pending orders: 2101, 2102, and 3001
        (is (true? (foreign-select-one [(keypath [1 1]) (view contains? 2101)] neworder-ps {:pkey 1})))
        (is (true? (foreign-select-one [(keypath [1 1]) (view contains? 2102)] neworder-ps {:pkey 1})))
        (is (true? (foreign-select-one [(keypath [1 1]) (view contains? 3001)] neworder-ps {:pkey 1})))

        ;; Read OL amounts for pending orders that will be delivered
        ;; D1: oldest pending = 2101 (c2), D2: oldest pending = 2101 (c3)
        (let [d1-order (foreign-select-one (keypath [1 1 2101]) order-ps {:pkey 1})
              d1-ol-total (tpcc/sum-orderline-amounts (:ols d1-order))
              d2-order (foreign-select-one (keypath [1 2 2101]) order-ps {:pkey 1})
              d2-ol-total (tpcc/sum-orderline-amounts (:ols d2-order))]

          ;; === Delivery on WH1 (oldest of multiple pending) ===
          ;; WH1/D1: pending 2101, 2102, 3001; WH1/D2: pending 2101; WH1/D3: no pending
          (let [req-id (h/make-request-id)]
            (foreign-append! tx-depot (types/->DeliveryTx 1 7 req-id))
            (swap! mb-count inc)
            (wait! ipc module-name @mb-count)

            ;; Neworder: oldest pending removed, others still pending
            (is (false? (foreign-select-one [(keypath [1 1]) (view contains? 2101)] neworder-ps {:pkey 1})))
            (is (true? (foreign-select-one [(keypath [1 1]) (view contains? 2102)] neworder-ps {:pkey 1})))
            (is (true? (foreign-select-one [(keypath [1 1]) (view contains? 3001)] neworder-ps {:pkey 1})))
            (is (false? (foreign-select-one [(keypath [1 2]) (view contains? 2101)] neworder-ps {:pkey 1})))

            ;; Orders: carrier-id set, delivery dates set
            (let [o1 (foreign-select-one (keypath [1 1 2101]) order-ps {:pkey 1})]
              (is (= 7 (:carrier o1)))
              (is (every? #(some? (:del-d %)) (:ols o1))))
            (let [o2 (foreign-select-one (keypath [1 2 2101]) order-ps {:pkey 1})]
              (is (= 7 (:carrier o2)))
              (is (every? #(some? (:del-d %)) (:ols o2))))

            ;; Customer 2 in WH1/D1: balance = -10 + d1-ol-total
            (is (= (+ -10.0 d1-ol-total)
                   (foreign-select-one (keypath [1 1 2] :bal) customer-ps {:pkey 1})))
            (is (= 1 (foreign-select-one (keypath [1 1 2] :del-cnt) customer-ps {:pkey 1})))
            ;; Customer 3 in WH1/D2: balance = -10 + d2-ol-total
            (is (= (+ -10.0 d2-ol-total)
                   (foreign-select-one (keypath [1 2 3] :bal) customer-ps {:pkey 1})))
            (is (= 1 (foreign-select-one (keypath [1 2 3] :del-cnt) customer-ps {:pkey 1})))

            ;; Delivery result: {1 2101, 2 2101} — D3 skipped (no pending)
            (let [res (foreign-select-one (keypath req-id) del-result-ps {:pkey 1})]
              (is (= {1 2101, 2 2101} res)))))

        ;; === Second delivery on WH1 (delivers next oldest: 2102 in D1) ===
        (let [d1-order-2102 (foreign-select-one (keypath [1 1 2102]) order-ps {:pkey 1})
              d1-ol-total-2102 (tpcc/sum-orderline-amounts (:ols d1-order-2102))
              ;; c2 balance was modified by first delivery above
              c2-balance-before (foreign-select-one (keypath [1 1 4] :bal) customer-ps {:pkey 1})
              req-id2 (h/make-request-id)]
          (foreign-append! tx-depot (types/->DeliveryTx 1 9 req-id2))
          (swap! mb-count inc)
          (wait! ipc module-name @mb-count)

          ;; D1: delivered 2102, D2: no more pending → only D1 in result
          (let [res (foreign-select-one (keypath req-id2) del-result-ps {:pkey 1})]
            (is (= {1 2102} res)))
          ;; Order 2102 now delivered with carrier-id=9
          (is (= 9 (foreign-select-one (keypath [1 1 2102] :carrier) order-ps {:pkey 1})))
          ;; Customer 4: balance = before + ol_total
          (is (= (+ c2-balance-before d1-ol-total-2102)
                 (foreign-select-one (keypath [1 1 4] :bal) customer-ps {:pkey 1})))
          (is (= 1 (foreign-select-one (keypath [1 1 4] :del-cnt) customer-ps {:pkey 1}))))

        ;; === Third delivery on WH1 (delivers 3001, then no more in D2/D3) ===
        (let [req-id2b (h/make-request-id)]
          (foreign-append! tx-depot (types/->DeliveryTx 1 8 req-id2b))
          (swap! mb-count inc)
          (wait! ipc module-name @mb-count)

          ;; Only 3001 from D1 delivered (OL amount = 10.0*2 = 20.0)
          (let [res (foreign-select-one (keypath req-id2b) del-result-ps {:pkey 1})]
            (is (= {1 3001} res)))
          (is (= 8 (foreign-select-one (keypath [1 1 3001] :carrier) order-ps {:pkey 1}))))

        ;; === Fourth delivery on WH1 (all consumed) ===
        (let [req-id2c (h/make-request-id)]
          (foreign-append! tx-depot (types/->DeliveryTx 1 8 req-id2c))
          (swap! mb-count inc)
          (wait! ipc module-name @mb-count)

          (let [res (foreign-select-one (keypath req-id2c) del-result-ps {:pkey 1})]
            (is (= {} res))))

        ;; === Delivery on WH2 ===
        ;; WH2/D1: pending order 2101 (c3), WH2/D2: pending order 2101 (c2)
        (let [wh2-d1-order (foreign-select-one (keypath [2 1 2101]) order-ps {:pkey 2})
              wh2-d1-total (tpcc/sum-orderline-amounts (:ols wh2-d1-order))
              wh2-d2-order (foreign-select-one (keypath [2 2 2101]) order-ps {:pkey 2})
              wh2-d2-total (tpcc/sum-orderline-amounts (:ols wh2-d2-order))
              req-id3 (h/make-request-id)]
          (foreign-append! tx-depot (types/->DeliveryTx 2 3 req-id3))
          (swap! mb-count inc)
          (wait! ipc module-name @mb-count)

          (let [res (foreign-select-one (keypath req-id3) del-result-ps {:pkey 2})]
            (is (= {1 2101, 2 2101} res)))
          ;; WH2/D1/C3: balance = -10 + total
          (is (= (+ -10.0 wh2-d1-total)
                 (foreign-select-one (keypath [2 1 3] :bal) customer-ps {:pkey 2})))
          (is (= 1 (foreign-select-one (keypath [2 1 3] :del-cnt) customer-ps {:pkey 2})))
          ;; WH2/D2/C2: balance = -10 + total
          (is (= (+ -10.0 wh2-d2-total)
                 (foreign-select-one (keypath [2 2 2] :bal) customer-ps {:pkey 2})))
          (is (= 1 (foreign-select-one (keypath [2 2 2] :del-cnt) customer-ps {:pkey 2}))))))))

(deftest order-status-test
  (with-redefs [tpcc/*test-mode* true]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [module-name       (get-module-name tpcc/TPCCModuleWithSeed)
            base-count        (setup-base-data! ipc module-name)
            _                 (wait! ipc module-name base-count)
            os-query          (foreign-query ipc module-name "order-status")]

        ;; === By customer ID: WH1/D1/C2 (pending order 2101) ===
        (let [res (foreign-invoke-query os-query 1 1 2 nil)]
          (is (= 2 (:c-id res)))
          (is (= -10.0 (:bal res)))
          (is (string? (:first res)))
          (is (= "OE" (:mid res)))
          ;; c-id=2 → gen-last-name(1) = BARBAROUGHT
          (is (= "BARBAROUGHT" (:last res)))
          ;; Customer 2's last order is 2101 (pending)
          (is (= 2101 (:o-id res)))
          (is (some? (:entry-d res)))
          (is (nil? (:carrier res)))
          ;; Pending order has random 5-15 orderlines
          (let [ol-count (count (:ols res))]
            (is (<= 5 ol-count 15)))
          ;; Pending orderlines have no delivery date
          (is (every? #(nil? (:del-d %)) (:ols res))))

        ;; === By last name: BARBARBAR in WH1/D1 ===
        ;; Only c-id=1 matches (unique names with small c-ids)
        (let [res (foreign-invoke-query os-query 1 1 nil "BARBARBAR")]
          (is (= 1 (:c-id res)))
          (is (string? (:first res))))

        ;; === By customer ID: delivered order (WH1/D1/C5, order 3) ===
        (let [res (foreign-invoke-query os-query 1 1 5 nil)]
          (is (= 5 (:c-id res)))
          (is (string? (:first res)))
          ;; c-id=5 → gen-last-name(4) = BARBARPRES
          (is (= "BARBARPRES" (:last res)))
          (is (= 3 (:o-id res)))
          (is (some? (:entry-d res)))
          ;; Delivered order has random carrier-id [1..10]
          (is (some? (:carrier res)))
          ;; Delivered order has 5-15 orderlines with delivery dates set
          (let [ol-count (count (:ols res))]
            (is (<= 5 ol-count 15)))
          (is (every? #(some? (:del-d %)) (:ols res)))
          ;; Delivered orderlines have amount=0.0
          (is (every? #(= 0.0 (:amt %)) (:ols res))))

        ;; === By customer ID: WH2/D1/C1 (delivered order 1) ===
        (let [res (foreign-invoke-query os-query 2 1 1 nil)]
          (is (= 1 (:c-id res)))
          (is (string? (:first res)))
          (is (= 1 (:o-id res))))))))

(deftest stock-level-test
  (with-redefs [tpcc/*test-mode* true
                dg/NUM-ITEMS 8]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [module-name   (get-module-name tpcc/TPCCModuleWithSeed)
            base-count    (setup-base-data! ipc module-name)
            _             (wait! ipc module-name base-count)
            ;; Override WH1/D1 next-o-id to 21 so orders 1-3 are in "last 20" window
            ;; Stock-level range: [next-o-id - 20, next-o-id) = [1, 21)
            district-depot (foreign-depot ipc module-name "*district-depot")
            _             (foreign-append! district-depot
                                           (types/->LoadDistrict 1 1 "D1" "st1" "st2" "city" "CA" "111111111" 0.05 30000.0 21))
            _             (wait! ipc module-name (inc base-count))
            sl-query      (foreign-query ipc module-name "stock-level")
            order-ps      (foreign-pstate ipc module-name "$$order")
            stock-ps      (foreign-pstate ipc module-name "$$stock")
            ;; Read actual seed orders in window [1, 21) for WH1/D1
            ;; These have random item IDs from gen-seed-order
            o1 (foreign-select-one (keypath [1 1 1]) order-ps {:pkey 1})
            o2 (foreign-select-one (keypath [1 1 2]) order-ps {:pkey 1})
            o3 (foreign-select-one (keypath [1 1 3]) order-ps {:pkey 1})
            all-item-ids (into #{} (comp (mapcat :ols) (map :i-id)) [o1 o2 o3])
            ;; Read actual stock quantities for these items
            item-qtys (into {} (map (fn [i-id]
                                      [i-id (foreign-select-one (keypath [1 i-id] :qty) stock-ps {:pkey 1})]))
                              all-item-ids)
            count-below (fn [threshold]
                          (count (filter #(< (val %) threshold) item-qtys)))]

        ;; threshold=10: all quantities are [10..100], none < 10
        (is (= 0 (foreign-invoke-query sl-query 1 1 10)))
        ;; threshold=101: all quantities ≤ 100, all below 101
        (is (= (count all-item-ids) (foreign-invoke-query sl-query 1 1 101)))
        ;; Dynamic thresholds verified against actual stock data
        (is (= (count-below 50) (foreign-invoke-query sl-query 1 1 50)))
        (is (= (count-below 30) (foreign-invoke-query sl-query 1 1 30)))
        (is (= (count-below 80) (foreign-invoke-query sl-query 1 1 80)))

        ;; === Stock-level on WH2 ===
        ;; Override WH2/D1 next-o-id to 21 so orders 1-2 fall in range [1, 21)
        (foreign-append! district-depot
                         (types/->LoadDistrict 1 2 "D2W1" "st1" "st2" "city" "NY" "111111111" 0.04 30000.0 21))
        (wait! ipc module-name (+ base-count 2))
        (let [wo1 (foreign-select-one (keypath [2 1 1]) order-ps {:pkey 2})
              wo2 (foreign-select-one (keypath [2 1 2]) order-ps {:pkey 2})
              wh2-item-ids (into #{} (comp (mapcat :ols) (map :i-id)) [wo1 wo2])
              wh2-qtys (into {} (map (fn [i-id]
                                       [i-id (foreign-select-one (keypath [2 i-id] :qty) stock-ps {:pkey 2})]))
                               wh2-item-ids)
              wh2-count-below (fn [threshold]
                                (count (filter #(< (val %) threshold) wh2-qtys)))]
          (is (= (count wh2-item-ids) (foreign-invoke-query sl-query 2 1 101)))
          (is (= 0 (foreign-invoke-query sl-query 2 1 10)))
          (is (= (wh2-count-below 50) (foreign-invoke-query sl-query 2 1 50))))))))

(deftest batch-test
  (with-redefs [tpcc/*test-mode* true]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [module-name   (get-module-name tpcc/TPCCModuleWithSeed)
            base-count    (setup-base-data! ipc module-name)
            _             (wait! ipc module-name base-count)
            tx-depot      (foreign-depot ipc module-name "*transaction-depot")
            district-ps   (foreign-pstate ipc module-name "$$district")
            customer-ps   (foreign-pstate ipc module-name "$$customer")
            order-ps      (foreign-pstate ipc module-name "$$order")
            neworder-ps   (foreign-pstate ipc module-name "$$neworder")
            stock-ps      (foreign-pstate ipc module-name "$$stock")
            warehouse-ytd-ps (foreign-pstate ipc module-name "$$warehouse-ytd")
            result-ps     (foreign-pstate ipc module-name "$$new-order-result")
            del-result-ps (foreign-pstate ipc module-name "$$delivery-result")
            cnt           (atom base-count)
            ;; Read initial stock for item 1 on WH1 (random [10..100])
            init-s1       (foreign-select-one (keypath [1 1] :qty) stock-ps {:pkey 1})]

        ;; === Two new-orders for same district in one microbatch ===
        ;; Verifies sequential o-id assignment and stacked stock updates
        (let [req-a (h/make-request-id)
              req-b (h/make-request-id)
              expected-s1-a (tpcc/compute-new-stock-qty init-s1 2)
              expected-s1-b (tpcc/compute-new-stock-qty expected-s1-a 3)]
          (rtest/pause-microbatch-topology! ipc module-name "tpcc")
          (foreign-append! tx-depot
                           (types/->NewOrderTx 1 1 3
                                               [(types/->OrderLineInput 1 1 2)]   ;; item 1, qty 2
                                               req-a))
          (swap! cnt inc)
          (foreign-append! tx-depot
                           (types/->NewOrderTx 1 1 4
                                               [(types/->OrderLineInput 1 1 3)]   ;; item 1, qty 3
                                               req-b))
          (swap! cnt inc)
          (rtest/resume-microbatch-topology! ipc module-name "tpcc")
          (wait! ipc module-name @cnt)

          ;; Both get sequential o-ids (order within microbatch is non-deterministic)
          (is (= 3003 (foreign-select-one (keypath [1 1] :next-oid) district-ps {:pkey 1})))
          (let [res-a (foreign-select-one (keypath (h/request-id-day-bucket req-a) req-a) result-ps {:pkey 1})
                res-b (foreign-select-one (keypath (h/request-id-day-bucket req-b) req-b) result-ps {:pkey 1})]
            (is (instance? NewOrderResult res-a))
            (is (instance? NewOrderResult res-b))
            (is (= #{3001 3002} #{(:o-id res-a) (:o-id res-b)}))
            ;; Each order's c-id matches its transaction
            (is (= 3 (foreign-select-one (keypath [1 1 (:o-id res-a)] :c-id) order-ps {:pkey 1})))
            (is (= 4 (foreign-select-one (keypath [1 1 (:o-id res-b)] :c-id) order-ps {:pkey 1}))))

          ;; Stock for item 1: init - 2 - 3, ytd = 5, order-cnt = 2
          (is (= expected-s1-b (foreign-select-one (keypath [1 1] :qty) stock-ps {:pkey 1})))
          (is (= 5 (foreign-select-one (keypath [1 1] :ytd) stock-ps {:pkey 1})))
          (is (= 2 (foreign-select-one (keypath [1 1] :ord-cnt) stock-ps {:pkey 1}))))

        ;; === Two payments for same customer in one microbatch ===
        (rtest/pause-microbatch-topology! ipc module-name "tpcc")
        (foreign-append! tx-depot
                         (types/->PaymentByIdTx 1 1 1 1 2 100.0 (h/make-request-id)))
        (swap! cnt inc)
        (foreign-append! tx-depot
                         (types/->PaymentByIdTx 1 1 1 1 2 50.0 (h/make-request-id)))
        (swap! cnt inc)
        (rtest/resume-microbatch-topology! ipc module-name "tpcc")
        (wait! ipc module-name @cnt)

        ;; Customer 2: balance -10-100-50=-160, payment-cnt 1+2=3
        (is (= -160.0 (foreign-select-one (keypath [1 1 2] :bal) customer-ps {:pkey 1})))
        (is (= 3 (foreign-select-one (keypath [1 1 2] :pay-cnt) customer-ps {:pkey 1})))
        ;; Warehouse ytd: 300000+100+50=300150
        (is (= 300150.0 (foreign-select-one (keypath 1) warehouse-ytd-ps)))
        ;; District ytd: 30000+100+50=30150
        (is (= 30150.0 (foreign-select-one (keypath [1 1] :ytd) district-ps {:pkey 1})))

        ;; === All three transaction types in one microbatch ===
        ;; New-order on WH2/D1, Payment on WH2, Delivery on WH2
        ;; WH2/D1: pending order 2101 (c3), WH2/D2: pending order 2101 (c2)
        ;; Read OL amounts for pending orders before delivery
        (let [wh2-d1-order (foreign-select-one (keypath [2 1 2101]) order-ps {:pkey 2})
              wh2-d1-total (tpcc/sum-orderline-amounts (:ols wh2-d1-order))
              wh2-d2-order (foreign-select-one (keypath [2 2 2101]) order-ps {:pkey 2})
              wh2-d2-total (tpcc/sum-orderline-amounts (:ols wh2-d2-order))
              no-req  (h/make-request-id)
              del-req (h/make-request-id)]
          (rtest/pause-microbatch-topology! ipc module-name "tpcc")
          (foreign-append! tx-depot
                           (types/->NewOrderTx 2 1 1
                                               [(types/->OrderLineInput 2 2 1)]   ;; item 2, supply WH2, qty 1
                                               no-req))
          (swap! cnt inc)
          (foreign-append! tx-depot
                           (types/->PaymentByIdTx 2 1 2 1 1 200.0 (h/make-request-id)))
          (swap! cnt inc)
          (foreign-append! tx-depot
                           (types/->DeliveryTx 2 5 del-req))
          (swap! cnt inc)
          (rtest/resume-microbatch-topology! ipc module-name "tpcc")
          (wait! ipc module-name @cnt)

          ;; New-order processed
          (let [no-res (foreign-select-one (keypath (h/request-id-day-bucket no-req) no-req) result-ps {:pkey 2})]
            (is (instance? NewOrderResult no-res)))

          ;; Payment processed: WH2 ytd increased by 200
          (is (= 300200.0 (foreign-select-one (keypath 2) warehouse-ytd-ps)))

          ;; Delivery processed: oldest pending orders delivered
          (let [del-res (foreign-select-one (keypath del-req) del-result-ps {:pkey 2})]
            (is (= 2101 (get del-res 1)))   ;; D1: delivered order 2101
            (is (= 2101 (get del-res 2))))  ;; D2: delivered order 2101

          ;; Neworder state: order 2101 removed from D1, 3001 added; order 2101 removed from D2
          (is (false? (foreign-select-one [(keypath [2 1]) (view contains? 2101)] neworder-ps {:pkey 2})))
          (is (true? (foreign-select-one [(keypath [2 1]) (view contains? 3001)] neworder-ps {:pkey 2})))
          (is (false? (foreign-select-one [(keypath [2 2]) (view contains? 2101)] neworder-ps {:pkey 2})))

          ;; Customer 1 on WH2/D1: payment of 200 → balance -10-200=-210
          (is (= -210.0 (foreign-select-one (keypath [2 1 1] :bal) customer-ps {:pkey 2})))
          ;; Customer 3 on WH2/D1: delivery of order 2101 → balance -10 + ol_total
          (is (= (+ -10.0 wh2-d1-total)
                 (foreign-select-one (keypath [2 1 3] :bal) customer-ps {:pkey 2})))
          ;; Customer 2 on WH2/D2: delivery of order 2101 → balance -10 + ol_total
          (is (= (+ -10.0 wh2-d2-total)
                 (foreign-select-one (keypath [2 2 2] :bal) customer-ps {:pkey 2}))))))))

(deftest seed-test
  (with-redefs [tpcc/*test-mode* true
                dg/NUM-ITEMS 100
                dg/DISTRICTS-PER-WH 3
                dg/CUSTOMERS-PER-DIST 30
                dg/ORDERS-PER-DIST 30
                dg/NEW-ORDER-START 22
                dg/ITEMS-PER-WH 100]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [tpcc-module-name (get-module-name tpcc/TPCCModuleWithSeed)
            num-warehouses 2
            num-clients 3]
        ;; Run 3 clients concurrently, each handling its subset
        (let [futures (mapv #(future (seed/gen-seed! ipc num-warehouses % num-clients 20))
                            (range num-clients))]
          (doseq [f futures] @f))
        ;; Wait for TPCCModule to process all appends
        (let [total-appends (+ dg/NUM-ITEMS
                               num-warehouses
                               (* num-warehouses dg/DISTRICTS-PER-WH)
                               (* num-warehouses (dg/customer-batches-per-wh))
                               (* num-warehouses (dg/stock-batches-per-wh)))]
          (wait! ipc tpcc-module-name total-appends))
        (let [items-ps     (foreign-pstate ipc tpcc-module-name "$$items")
              warehouse-ytd-ps  (foreign-pstate ipc tpcc-module-name "$$warehouse-ytd")
              warehouse-info-ps (foreign-pstate ipc tpcc-module-name "$$warehouse-info")
              district-ps  (foreign-pstate ipc tpcc-module-name "$$district")
              customer-ps  (foreign-pstate ipc tpcc-module-name "$$customer")
              cust-last-ps (foreign-pstate ipc tpcc-module-name "$$customer-by-last")
              stock-ps     (foreign-pstate ipc tpcc-module-name "$$stock")
              order-ps     (foreign-pstate ipc tpcc-module-name "$$order")
              neworder-ps  (foreign-pstate ipc tpcc-module-name "$$neworder")
              history-ps   (foreign-pstate ipc tpcc-module-name "$$history")]

          ;; $$items — all 100 items present
          (doseq [i (range 1 101)]
            (is (some? (foreign-select-one (keypath i) items-ps))
                (str "Item " i " should exist")))
          (is (nil? (foreign-select-one (keypath 101) items-ps)))

          ;; $$warehouse-ytd + $$warehouse-info — both warehouses present
          (doseq [w-id [1 2]]
            (is (= 300000.0 (foreign-select-one (keypath w-id) warehouse-ytd-ps))
                (str "Warehouse " w-id " ytd should exist"))
            (is (some? (foreign-select-one (keypath w-id) warehouse-info-ps))
                (str "Warehouse " w-id " info should exist")))
          (is (nil? (foreign-select-one (keypath 3) warehouse-ytd-ps)))

          ;; $$district — 3 districts per warehouse
          (doseq [w-id [1 2]
                  d-id [1 2 3]]
            (let [dist (foreign-select-one (keypath [w-id d-id]) district-ps {:pkey w-id})]
              (is (some? dist) (str "District " w-id "/" d-id " should exist"))
              (is (= 31 (:next-oid dist)))))

          ;; $$customer — all 30 customers per district, both warehouses
          (doseq [w-id [1 2]
                  d-id [1 2 3]]
            (doseq [c-id [1 15 30]]
              (is (some? (foreign-select-one (keypath [w-id d-id c-id]) customer-ps {:pkey w-id}))
                  (str "Customer " w-id "/" d-id "/" c-id " should exist")))
            (is (nil? (foreign-select-one (keypath [w-id d-id 31]) customer-ps {:pkey w-id}))))

          ;; $$customer-by-last — BARBARBAR exists in each district
          (doseq [w-id [1 2]]
            (let [matches (foreign-select-one (keypath [w-id 1 "BARBARBAR"]) cust-last-ps {:pkey w-id})]
              (is (some? matches))
              (is (pos? (count matches)))))

          ;; $$stock — all 100 items per warehouse
          (doseq [w-id [1 2]]
            (doseq [i-id [1 50 100]]
              (is (some? (foreign-select-one (keypath [w-id i-id]) stock-ps {:pkey w-id}))
                  (str "Stock " w-id "/" i-id " should exist")))
            (is (nil? (foreign-select-one (keypath [w-id 101]) stock-ps {:pkey w-id}))))

          ;; $$stock-detail — dist-info has 10 entries
          (let [stock-detail-ps (foreign-pstate ipc tpcc-module-name "$$stock-detail")]
            (doseq [w-id [1 2]]
              (doseq [i-id [1 50 100]]
                (let [sd (foreign-select-one (keypath [w-id i-id]) stock-detail-ps {:pkey w-id})]
                  (is (some? sd) (str "Stock-detail " w-id "/" i-id " should exist"))
                  (is (= 10 (count (:dinfo sd))))))))

          ;; $$order — all 30 orders per district, both warehouses
          (doseq [w-id [1 2]
                  d-id [1 2 3]]
            ;; Delivered order
            (let [o1 (foreign-select-one (keypath [w-id d-id 1]) order-ps {:pkey w-id})]
              (is (some? o1) (str "Order " w-id "/" d-id "/1 should exist"))
              (is (some? (:carrier o1))))
            ;; Pending order
            (let [op (foreign-select-one (keypath [w-id d-id 30]) order-ps {:pkey w-id})]
              (is (some? op) (str "Order " w-id "/" d-id "/30 should exist"))
              (is (nil? (:carrier op)))))

          ;; $$neworder — 9 pending per district per warehouse
          (doseq [w-id [1 2]
                  d-id [1 2 3]]
            (is (= 9 (foreign-select-one [(keypath [w-id d-id]) (view count)] neworder-ps {:pkey w-id}))
                (str "Neworder " w-id "/" d-id " should have 9")))

          ;; $$history — 30 per district per warehouse
          (doseq [w-id [1 2]
                  d-id [1 2 3]]
            (is (= 30 (foreign-select-one [(keypath [w-id d-id]) (view count)] history-ps {:pkey w-id}))
                (str "History " w-id "/" d-id " should have 30")))

          ;; $$customer :last-oid — every customer has an order assigned
          (doseq [w-id [1 2]
                  d-id [1 2 3]
                  c-id [1 15 30]]
            (let [last-oid (foreign-select-one (keypath [w-id d-id c-id] :last-oid)
                                               customer-ps {:pkey w-id})]
              (is (some? last-oid) (str "Customer " w-id "/" d-id "/" c-id " should have last-o-id"))
              (is (>= last-oid 1))
              (is (<= last-oid 30)))))))))

(deftest load-runner-test
  (with-redefs [tpcc/*test-mode* true
                dg/NUM-ITEMS 100
                dg/DISTRICTS-PER-WH 3
                dg/CUSTOMERS-PER-DIST 30
                dg/ORDERS-PER-DIST 30
                dg/NEW-ORDER-START 22
                dg/ITEMS-PER-WH 100]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [tpcc-module-name (get-module-name tpcc/TPCCModuleWithSeed)
            num-warehouses 2]
        ;; Seed data
        (seed/gen-seed! ipc num-warehouses 0 1 20)
        (let [total-appends (+ dg/NUM-ITEMS
                               num-warehouses
                               (* num-warehouses dg/DISTRICTS-PER-WH)
                               (* num-warehouses (dg/customer-batches-per-wh))
                               (* num-warehouses (dg/stock-batches-per-wh)))]
          (wait! ipc tpcc-module-name total-appends))
        ;; Run load: 6 terminals (2 wh × 3 dist), zero delays, 60 seconds
        ;; Terminal model waits for microbatch response (~1-4s per cycle), so throughput
        ;; is modest (~30-100 txns). 60s ensures 2+ full decks (23 cards each) complete.
        ;; Mix validation uses max-throughput-test; here we verify basic mechanics.
        (let [result (lr/run-load! (lr/make-load-context ipc) num-warehouses 60 0 1
                                   :time-scale 0.0)
              {:keys [total-count no-count mix latencies]} result
              {:keys [pay-count os-count]} mix]
          ;; Transaction counts — all types present after 2+ full decks
          (is (pos? total-count))
          (is (pos? no-count))
          (is (pos? pay-count))
          (is (pos? os-count))
          ;; All latency categories should have data
          (is (some? (:no-initiate latencies)))
          (is (some? (:no-complete latencies)))
          (is (some? (:pay-initiate latencies)))
          (is (some? (:pay-complete latencies)))
          (is (some? (:del-initiate latencies)))
          (is (some? (:order-status latencies)))
          (is (some? (:stock-level latencies)))
          ;; Latency values should be positive
          (doseq [[k stats] latencies]
            (when stats
              (is (pos? (:n stats)) (str k " should have samples"))
              (is (pos? (:p50 stats)) (str k " p50 should be positive")))))))))

(deftest max-throughput-test
  (with-redefs [tpcc/*test-mode* true
                dg/NUM-ITEMS 100
                dg/DISTRICTS-PER-WH 3
                dg/CUSTOMERS-PER-DIST 30
                dg/ORDERS-PER-DIST 30
                dg/NEW-ORDER-START 22
                dg/ITEMS-PER-WH 100]
    (with-open [ipc (rtest/create-ipc)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [tpcc-module-name (get-module-name tpcc/TPCCModuleWithSeed)
            num-warehouses 2]
        ;; Seed data
        (seed/gen-seed! ipc num-warehouses 0 1 20)
        (let [total-appends (+ dg/NUM-ITEMS
                               num-warehouses
                               (* num-warehouses dg/DISTRICTS-PER-WH)
                               (* num-warehouses (dg/customer-batches-per-wh))
                               (* num-warehouses (dg/stock-batches-per-wh)))]
          (wait! ipc tpcc-module-name total-appends))
        ;; Run max-throughput load: 6 terminals, 15 seconds, 50 max-pending
        (let [result (lr/run-load-max! (lr/make-load-context ipc) num-warehouses 15 0 1 50)
              {:keys [total-count no-count mix latencies]} result
              {:keys [no-remote-ol no-total-ol pay-count pay-remote
                      pay-by-last os-count os-by-last]} mix]
          ;; Transaction counts
          (is (pos? total-count))
          (is (pos? no-count))
          (is (pos? pay-count))
          (is (pos? os-count))
          ;; Deck ratio: 10/23 NO, 10/23 PAY, 1/23 each for DEL/OS/SL
          (let [no-pct (* 100.0 (/ no-count (double total-count)))
                pay-pct (* 100.0 (/ pay-count (double total-count)))]
            (is (< 35 no-pct 55) (str "NO% should be ~43%: " no-pct))
            (is (< 35 pay-pct 55) (str "PAY% should be ~43%: " pay-pct)))
          ;; Mix validation — wide ranges for small sample sizes
          (when (pos? no-total-ol)
            (let [remote-ol-pct (* 100.0 (/ (double no-remote-ol) no-total-ol))]
              (is (< remote-ol-pct 5) (str "Remote OL% too high: " remote-ol-pct))))
          (when (pos? pay-count)
            (let [remote-pct (* 100.0 (/ (double pay-remote) pay-count))
                  by-last-pct (* 100.0 (/ (double pay-by-last) pay-count))]
              (is (< 3 remote-pct 35) (str "PAY remote% out of range: " remote-pct))
              (is (< 35 by-last-pct 85) (str "PAY by-last% out of range: " by-last-pct))))
          (when (pos? os-count)
            (let [by-last-pct (* 100.0 (/ (double os-by-last) os-count))]
              (is (< 15 by-last-pct 95) (str "OS by-last% out of range: " by-last-pct))))
          ;; All latency categories should have data
          (is (some? (:no-initiate latencies)))
          (is (some? (:no-complete latencies)))
          (is (some? (:pay-initiate latencies)))
          (is (some? (:pay-complete latencies)))
          (is (some? (:del-initiate latencies)))
          (is (some? (:order-status latencies)))
          (is (some? (:stock-level latencies)))
          ;; Latency values should be positive
          (doseq [[k stats] latencies]
            (when stats
              (is (pos? (:n stats)) (str k " should have samples"))
              (is (pos? (:p50 stats)) (str k " p50 should be positive")))))))))

(deftest cleanup-test
  (with-redefs [tpcc/*test-mode* true]
    (with-open [ipc (rtest/create-ipc)
                _   (TopologyUtils/startSimTime)]
      (rtest/launch-module! ipc tpcc/TPCCModuleWithSeed {:tasks (rand-tasks) :threads 2})
      (let [module-name    (get-module-name tpcc/TPCCModuleWithSeed)
            base-count     (setup-base-data! ipc module-name)
            _              (wait! ipc module-name base-count)
            tx-depot       (foreign-depot ipc module-name "*transaction-depot")
            cleanup-depot  (foreign-depot ipc module-name "*cleanup-tick")
            result-ps      (foreign-pstate ipc module-name "$$new-order-result")
            pay-result-ps  (foreign-pstate ipc module-name "$$payment-result")
            mb-count       (atom base-count)
            ;; Helper: submit one NO and one PAY, return {:no-req :pay-req :no-day :pay-day}
            submit-txs! (fn [c-id]
                          (let [no-req  (h/make-request-id)
                                pay-req (h/make-request-id)]
                            (foreign-append! tx-depot
                                             (types/->NewOrderTx 1 1 c-id
                                                                 [(types/->OrderLineInput 1 1 1)]
                                                                 no-req))
                            (swap! mb-count inc)
                            (foreign-append! tx-depot
                                             (types/->PaymentByIdTx 1 1 1 1 c-id 10.0 pay-req))
                            (swap! mb-count inc)
                            (wait! ipc module-name @mb-count)
                            {:no-req no-req :pay-req pay-req
                             :no-day (h/request-id-day-bucket no-req)
                             :pay-day (h/request-id-day-bucket pay-req)}))]

        ;; Create data on 5 consecutive days (days 0-4)
        ;; Sim time starts at 0, so day 0
        (let [day0 (submit-txs! 1)
              _    (TopologyUtils/advanceSimTime h/DAY-MILLIS)
              day1 (submit-txs! 2)
              _    (TopologyUtils/advanceSimTime h/DAY-MILLIS)
              day2 (submit-txs! 3)
              _    (TopologyUtils/advanceSimTime h/DAY-MILLIS)
              day3 (submit-txs! 4)
              _    (TopologyUtils/advanceSimTime h/DAY-MILLIS)
              day4 (submit-txs! 5)]

          ;; Verify all 5 days of data exist
          (doseq [d [day0 day1 day2 day3 day4]]
            (is (some? (foreign-select-one (keypath (:no-day d) (:no-req d)) result-ps {:pkey 1})))
            (is (some? (foreign-select-one (keypath (:pay-day d) (:pay-req d)) pay-result-ps {:pkey 1}))))

          ;; Trigger cleanup at day 4
          ;; Cutoff = 4 - 1 = 3, sorted-map-range [0, 3) removes days 0, 1, 2
          ;; Days 3 (yesterday) and 4 (today) survive
          (foreign-append! cleanup-depot 1)
          (swap! mb-count inc)
          (wait! ipc module-name @mb-count)

          ;; Days 0-2: entire buckets gone from PStates
          (doseq [d [day0 day1 day2]]
            (is (false? (foreign-select-one [(view contains? (:no-day d))] result-ps {:pkey 1}))
                (str "NO result bucket for day " (:no-day d) " should be removed"))
            (is (false? (foreign-select-one [(view contains? (:pay-day d))] pay-result-ps {:pkey 1}))
                (str "PAY result bucket for day " (:pay-day d) " should be removed")))

          ;; Days 3 and 4: data still exists
          (doseq [d [day3 day4]]
            (is (true? (foreign-select-one [(view contains? (:no-day d))] result-ps {:pkey 1}))
                (str "NO result bucket for day " (:no-day d) " should survive"))
            (is (true? (foreign-select-one [(view contains? (:pay-day d))] pay-result-ps {:pkey 1}))
                (str "PAY result bucket for day " (:pay-day d) " should survive"))
            (is (some? (foreign-select-one (keypath (:no-day d) (:no-req d)) result-ps {:pkey 1})))
            (is (some? (foreign-select-one (keypath (:pay-day d) (:pay-req d)) pay-result-ps {:pkey 1}))))

          ;; Advance one more day (day 5), add data, cleanup again
          ;; Cutoff = 5 - 1 = 4, sorted-map-range [0, 4) removes days 0-3
          ;; Days 4 (yesterday) and 5 (today) survive
          (TopologyUtils/advanceSimTime h/DAY-MILLIS)
          (let [day5 (submit-txs! 1)]
            (foreign-append! cleanup-depot 1)
            (swap! mb-count inc)
            (wait! ipc module-name @mb-count)

            ;; Day 3: now cleaned up
            (is (false? (foreign-select-one [(view contains? (:no-day day3))] result-ps {:pkey 1})))
            (is (false? (foreign-select-one [(view contains? (:pay-day day3))] pay-result-ps {:pkey 1})))

            ;; Days 4 and 5: survive
            (doseq [d [day4 day5]]
              (is (true? (foreign-select-one [(view contains? (:no-day d))] result-ps {:pkey 1}))
                  (str "NO result bucket for day " (:no-day d) " should survive"))
              (is (true? (foreign-select-one [(view contains? (:pay-day d))] pay-result-ps {:pkey 1}))
                  (str "PAY result bucket for day " (:pay-day d) " should survive"))
              (is (some? (foreign-select-one (keypath (:no-day d) (:no-req d)) result-ps {:pkey 1})))
              (is (some? (foreign-select-one (keypath (:pay-day d) (:pay-req d)) pay-result-ps {:pkey 1}))))))))))
