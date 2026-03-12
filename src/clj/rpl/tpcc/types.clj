(ns rpl.tpcc.types
  (:require [rpl.rama.util.helpers :as h]
            [rpl.schema.core :as s]))


(defmacro deffastrecord
  [name fields]
  (let [annotated (vec (mapcat (fn [f] [f :- s/Any]) fields))]
    `(h/defrecord+ ~name ~annotated)))

;; Transaction depot records
(deffastrecord OrderLineInput [i-id sup-wid qty])

(deffastrecord NewOrderTx
  [w-id d-id c-id
   ols                        ;; vector of OrderLineInput
   request-id])

(deffastrecord PaymentByIdTx
  [w-id d-id c-w-id c-d-id
   c-id                     ;; customer ID for direct lookup
   h-amt                    ;; payment amount
   request-id])

(deffastrecord PaymentByLastTx
  [w-id d-id c-w-id c-d-id
   last                     ;; customer last name for lookup
   h-amt                    ;; payment amount
   request-id])

(deffastrecord DeliveryTx
  [w-id carrier request-id])

;; Seed data records (initial population, no transaction equivalent)
(deffastrecord LoadItem      [id im-id name price data])
(deffastrecord LoadWarehouse [w-id name st1 st2 city state zip tax ytd])
(deffastrecord LoadDistrict  [id w-id name st1 st2 city state zip tax ytd next-oid])
(deffastrecord LoadCustomer  [w-id d-id start-id o-ids])
(deffastrecord LoadStock     [w-id start-id amt])

;; Result records (efficient serialization — no key strings)
(deffastrecord NewOrderResult
  [o-id entry-d disc last cr items])

(deffastrecord NewOrderInvalidResult
  [o-id last cr])

(deffastrecord PaymentCustomerInfo
  [first mid last
   st1 st2 city state zip
   phone since])

(deffastrecord PaymentResult
  [c-id customer h-date
   cr cr-lim disc bal data])

(deffastrecord OrderStatusResult
  [c-id bal first mid last
   o-id entry-d carrier ols])
