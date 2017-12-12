(ns metabase.mbql
  (:refer-clojure :exclude [min max])
  (:require [schema.core :as s]))

(defprotocol MBQL
  (->mbql [this]))

(extend-protocol MBQL
  Object
  (->mbql [this] this)
  nil
  (->mbql [_] nil))

(defmethod print-method metabase.mbql.MBQL [s writer]
  (print-method (->mbql s) writer))

(defmethod clojure.pprint/simple-dispatch metabase.mbql.MBQL [s]
  (clojure.pprint/write-out (->mbql s)))

(doseq [m [print-method clojure.pprint/simple-dispatch]]
  (prefer-method m metabase.mbql.MBQL clojure.lang.IRecord)
  (prefer-method m metabase.mbql.MBQL java.util.Map)
  (prefer-method m metabase.mbql.MBQL clojure.lang.IPersistentMap))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Fields/ETc.                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

(s/defrecord Field [id :- s/Num]
  MBQL
  (->mbql [_] (list 'field id)))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Aggregations                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                   Breakouts                                                    |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Expressions                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                    Filters                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

(declare Filter)

(s/defrecord AndFilter [subclauses :- [(s/recursive #'Filter)]]
  MBQL
  (->mbql [_] (cons 'and (map ->mbql subclauses))))

(s/defrecord OrFilter [subclauses :- [(s/recursive #'Filter)]]
  MBQL
  (->mbql [_] (cons 'or (map ->mbql subclauses))))

(s/defrecord NotFilter [subclause :- (s/recursive #'Filter)]
  MBQL
  (->mbql [_] (list 'not (->mbql subclause))))

(defrecord EqualsFilter [x y]
  MBQL
  (->mbql [_] (list '= (->mbql x) (->mbql y))))

(defrecord NotEqualsFilter [x y]
  MBQL
  (->mbql [_] (list '!= (->mbql x) (->mbql y))))

(defrecord LessThanFilter [x y]
  MBQL
  (->mbql [_] (list '< (->mbql x) (->mbql y))))

(defrecord LessThanOrEqualFilter [x y]
  MBQL
  (->mbql [_] (list '<= (->mbql x) (->mbql y))))

(defrecord GreaterThanFilter [x y]
  MBQL
  (->mbql [_] (list '> (->mbql x) (->mbql y))))

(defrecord GreaterThanOrEqualFilter [x y]
  MBQL
  (->mbql [_] (list '>= (->mbql x) (->mbql y))))

(defrecord BetweenFilter [x min max]
  MBQL
  (->mbql [_] (list 'between (->mbql x) (->mbql min) (->mbql max))))


(def Filter
  (s/cond-pre AndFilter
              OrFilter
              NotFilter
              EqualsFilter
              NotEqualsFilter
              LessThanFilter
              LessThanOrEqualFilter
              GreaterThanFilter
              GreaterThanOrEqualFilter
              BetweenFilter))

;; !=
;; <
;; >
;; <=
;; >=
;; [is-null field]
;; [not-null field]
;; [between field min max]
;; [inside lat lon lat-max lon-min lat-min lon-max]
;; [starts-with field str]
;; [contains field str]
;; [does-not-contain field str]
;; [ends-with field str]
;; [time-interval field current|last|next|n unit & [options]]


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     Limits                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                    Order By                                                    |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                      Page                                                      |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Source Table                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Source Query                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     Query                                                      |
;;; +----------------------------------------------------------------------------------------------------------------+


(defrecord Query []
  MBQL
  (->mbql [this]
    (into {} (for [[k v] this]
               {k (->mbql v)}))))
