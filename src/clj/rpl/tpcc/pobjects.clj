(ns rpl.tpcc.pobjects
  (:use [com.rpl.rama])
  (:import [java.util List Map UUID]))

;; ---------------------------------------------------------------------------
;; PState names and schemas
;; ---------------------------------------------------------------------------

;; {item-id -> {...}}
(def ITEMS-NAME "$$items")
(def ITEMS-SCHEMA
  {Long (fixed-keys-schema {:im-id Long
                            :name  String
                            :price Double
                            :data  String})})

;; {w-id -> ytd} — mutable field only
(def WAREHOUSE-YTD-NAME "$$warehouse-ytd")
(def WAREHOUSE-YTD-SCHEMA
  {Long Double})

;; {w-id -> {...}} — immutable fields (cached in-memory)
(def WAREHOUSE-INFO-NAME "$$warehouse-info")
(def WAREHOUSE-INFO-SCHEMA
  {Long (fixed-keys-schema {:name  String
                            :st1   String
                            :st2   String
                            :city  String
                            :state String
                            :zip   String
                            :tax   Double})})

;; {[w-id d-id] -> {...}} — mutable fields only
(def DISTRICT-NAME "$$district")
(def DISTRICT-SCHEMA
  {List (fixed-keys-schema {:ytd      Double
                            :next-oid Long})})

;; {[w-id d-id] -> {...}} — immutable fields (cached in-memory)
(def DISTRICT-INFO-NAME "$$district-info")
(def DISTRICT-INFO-SCHEMA
  {List (fixed-keys-schema {:name  String
                            :st1   String
                            :st2   String
                            :city  String
                            :state String
                            :zip   String
                            :tax   Double})})

;; {[w-id d-id c-id] -> {...}} — hot fields only
(def CUSTOMER-NAME "$$customer")
(def CUSTOMER-SCHEMA
  {List (fixed-keys-schema {:first   String
                            :mid     String
                            :last    String
                            :cr      String
                            :disc    Double
                            :bal     Double
                            :ytd-pay Double
                            :pay-cnt Long
                            :del-cnt Long
                            :last-oid Long})})

;; {[w-id d-id c-id] -> {...}} — cold fields (address, phone, data)
(def CUSTOMER-DETAIL-NAME "$$customer-detail")
(def CUSTOMER-DETAIL-SCHEMA
  {List (fixed-keys-schema {:st1     String
                            :st2     String
                            :city    String
                            :state   String
                            :zip     String
                            :phone   String
                            :since   Long
                            :cr-lim  Double
                            :data    String})})

;; {[w-id d-id last-name] -> sorted vector of [first-name c-id]}
(def CUSTOMER-BY-LAST-NAME "$$customer-by-last")
(def CUSTOMER-BY-LAST-SCHEMA
  {List (vector-schema java.util.List)})

;; {[w-id d-id] -> map of history records} (append-only)
(def HISTORY-NAME "$$history")
(def HISTORY-SCHEMA
  {List (map-schema
          UUID
          (fixed-keys-schema {:c-id   Long
                              :c-d-id Long
                              :c-w-id Long
                              :d-id   Long
                              :w-id   Long
                              :date   Long
                              :amt    Double
                              :data   String})
          {:subindex-options {:track-size? false}})})

;; {[w-id d-id o-id] -> {...}}
(def ORDER-NAME "$$order")
(def ORDER-SCHEMA
  {List (fixed-keys-schema {:c-id    Long
                            :entry-d Long
                            :carrier Long
                            :local?  Boolean
                            :ols     (vector-schema
                                      (fixed-keys-schema {:i-id    Long
                                                          :sup-wid Long
                                                          :del-d   Long
                                                          :qty     Long
                                                          :amt     Double
                                                          :dinfo   String}))})})

;; {[w-id d-id] -> sorted-set of o-id} (queue of undelivered orders)
(def NEWORDER-NAME "$$neworder")
(def NEWORDER-SCHEMA
  {List (set-schema Long {:subindex-options {:track-size? false}})})

;; {[w-id i-id] -> {...}} — hot fields only
(def STOCK-NAME "$$stock")
(def STOCK-SCHEMA
  {List (fixed-keys-schema {:qty     Long
                            :ytd     Long
                            :ord-cnt Long
                            :rem-cnt Long})})

;; {[w-id i-id] -> {...}} — cold fields (dist-info, data)
(def STOCK-DETAIL-NAME "$$stock-detail")
(def STOCK-DETAIL-SCHEMA
  {List (fixed-keys-schema {:dinfo (vector-schema String)
                            :data  String})})

;; {day-bucket -> request-id -> result map}
(def NEW-ORDER-RESULT-NAME "$$new-order-result")
(def NEW-ORDER-RESULT-SCHEMA
  {Long (map-schema UUID Map {:subindex-options {:track-size? false}})})

;; {request-id -> {d-id -> o-id}}
(def DELIVERY-RESULT-NAME "$$delivery-result")
(def DELIVERY-RESULT-SCHEMA
  {UUID Map})

;; {day-bucket -> request-id -> result map}
(def PAYMENT-RESULT-NAME "$$payment-result")
(def PAYMENT-RESULT-SCHEMA
  {Long (map-schema UUID Map {:subindex-options {:track-size? false}})})

;; ---------------------------------------------------------------------------
;; Task global accessors (for use with <<with-substitutions)
;; ---------------------------------------------------------------------------

(defn items-task-global             [] (this-module-pobject-task-global ITEMS-NAME))
(defn warehouse-ytd-task-global      [] (this-module-pobject-task-global WAREHOUSE-YTD-NAME))
(defn warehouse-info-task-global    [] (this-module-pobject-task-global WAREHOUSE-INFO-NAME))
(defn district-task-global          [] (this-module-pobject-task-global DISTRICT-NAME))
(defn district-info-task-global     [] (this-module-pobject-task-global DISTRICT-INFO-NAME))
(defn customer-task-global          [] (this-module-pobject-task-global CUSTOMER-NAME))
(defn customer-by-last-task-global  [] (this-module-pobject-task-global CUSTOMER-BY-LAST-NAME))
(defn history-task-global           [] (this-module-pobject-task-global HISTORY-NAME))
(defn order-task-global             [] (this-module-pobject-task-global ORDER-NAME))
(defn neworder-task-global          [] (this-module-pobject-task-global NEWORDER-NAME))
(defn stock-task-global             [] (this-module-pobject-task-global STOCK-NAME))
(defn stock-detail-task-global      [] (this-module-pobject-task-global STOCK-DETAIL-NAME))
(defn customer-detail-task-global   [] (this-module-pobject-task-global CUSTOMER-DETAIL-NAME))
(defn new-order-result-task-global  [] (this-module-pobject-task-global NEW-ORDER-RESULT-NAME))
(defn delivery-result-task-global  [] (this-module-pobject-task-global DELIVERY-RESULT-NAME))
(defn payment-result-task-global   [] (this-module-pobject-task-global PAYMENT-RESULT-NAME))
(defn processed-items-task-global   [] (this-module-pobject-task-global "$$processed-items"))
