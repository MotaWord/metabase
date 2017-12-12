(ns metabase.mbql.parse
  (:refer-clojure :exclude [< <= > >= = != and or not filter count distinct sum min max + - / *])
  (:require [metabase.mbql :as mbql]
            [schema.core :as s])
  (:import [metabase.mbql
            Field
            AndFilter
            OrFilter
            NotFilter
            EqualsFilter
            NotEqualsFilter
            LessThanFilter
            LessThanOrEqualFilter
            GreaterThanFilter
            GreaterThanOrEqualFilter
            BetweenFilter]))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Fields/Etc.                                                   |
;;; +----------------------------------------------------------------------------------------------------------------+

(s/defn field-id :- Field
  [id :- s/Num]
  (mbql/map->Field {:id id}))

(s/defn field :- Field
  [id :- s/Num]
  (mbql/map->Field {:id id}))

(s/defn ->Field :- Field
  [x]
  (cond
    (integer? x)
    (field-id x)

    (instance? Field x)
    x))

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

(defn compound-filter [klass map->filter-fn subclauses]
  (let [subclauses (for [subclause subclauses
                         subclause (if (instance? klass subclause)
                                     (:subclauses subclause)
                                     [subclause])]
                     subclause)]
    (if-not (seq (rest subclauses))
      (first subclauses)
      (map->filter-fn {:subclauses subclauses} ))))

(s/defn and :- mbql/Filter
  {:style/indent 0}
  [& subclauses]
  (compound-filter AndFilter mbql/map->AndFilter subclauses))

(s/defn or :- mbql/Filter
  {:style/indent 0}
  [& subclauses]
  (compound-filter OrFilter mbql/map->OrFilter subclauses))

(declare = < <= > >=)

(s/defn not :- mbql/Filter
  {:style/indent 0}
  [subclause]
  (cond
    (instance? NotFilter subclause)
    (:subclause subclause)

    (instance? NotEqualsFilter subclause)
    (= (:x subclause) (:y subclause))

    (instance? EqualsFilter subclause)
    (!= (:x subclause) (:y subclause))

    (instance? LessThanFilter subclause)
    (>= (:x subclause) (:y subclause))

    (instance? GreaterThanFilter subclause)
    (<= (:x subclause) (:y subclause))

    (instance? LessThanOrEqualFilter subclause)
    (> (:x subclause) (:y subclause))

    (instance? GreaterThanOrEqualFilter subclause)
    (< (:x subclause) (:y subclause))

    :else
    (mbql/map->NotFilter {:subclause subclause})))


(s/defn = :- (s/cond-pre EqualsFilter OrFilter)
  ([x y]
   (mbql/map->EqualsFilter {:x x, :y y}))
  ([x y & more]
   (or (= x y)
       (apply = x more))))

(s/defn != :- (s/cond-pre NotEqualsFilter AndFilter)
  ([x y]
   (mbql/map->NotEqualsFilter {:x x, :y y}))
  ([x y & more]
   (and (!= x y)
        (apply != x more))))


(defn- ordered-filter
  ([map->filter-fn x y]
   (map->filter-fn {:x x, :y y}))
  ([map->filter-fn x y & more]
   (and (ordered-filter map->filter-fn x y)
        (apply ordered-filter map->filter-fn y more))))

(s/defn < :- (s/cond-pre LessThanFilter AndFilter) [x y & more]
  (apply ordered-filter mbql/map->LessThanFilter x y more))

(s/defn <= :- (s/cond-pre LessThanOrEqualFilter AndFilter) [x y & more]
  (apply ordered-filter mbql/map->LessThanOrEqualFilter x y more))

(s/defn > :- (s/cond-pre GreaterThanFilter AndFilter) [x y & more]
  (apply ordered-filter mbql/map->GreaterThanFilter x y more))

(s/defn >= :- (s/cond-pre GreaterThanOrEqualFilter AndFilter) [x y & more]
  (apply ordered-filter mbql/map->GreaterThanOrEqualFilter x y more))


(s/defn is-null :- EqualsFilter
  [field]
  (= (->Field field) nil))

(s/defn not-null :- NotEqualsFilter
  [field]
  (!= (->Field field) nil))

(s/defn between :- BetweenFilter
  [x min max]
  (mbql/map->BetweenFilter {:x (->Field x), :min min, :max max}))



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


(defn query {:style/indent 0} [& {:as clauses}]
  (mbql/map->Query clauses))
