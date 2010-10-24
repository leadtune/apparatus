(ns apparatus.test
  (:use [apparatus.config]
        [clojure.test])
  (:require [apparatus.cluster :as cluster])
  (:import [java.util UUID]))

(defn many [f] (take 4 (repeatedly f)))

(defn uuid [] (str (UUID/randomUUID)))

(deftest apparatus
  (testing "apparatus"
    (let [config (-> (config) (config-group (uuid) (uuid)))
          instances (doall (many #(cluster/instance config)))]
      (testing "should be able to eval an sexp"
        (testing "on randomly selected member"
          (doall
           (many
            #(let [sexp `(* ~(rand) ~(rand-int 100))]
               (is (= (eval sexp) (-> (cluster/eval-any sexp) (.get))))))))
        (testing "on a member that owns a key"
          (doall
           (many
            #(let [uuid (uuid)
                   sexp `(* ~(rand) ~(rand-int 100))]
               (-> (cluster/get-set "test") (.add uuid))
               (is (= (eval sexp)
                      (-> (cluster/eval-on sexp uuid) (.get))))))))
        (testing "on a member by reference"
          (doall
           (many
            #(let [sexp `(* ~(rand) ~(rand-int 100))]
               (is (= (eval sexp)
                      (-> (cluster/eval-on sexp (first (cluster/members)))
                          (.get))))))))
        (testing "on some of the members"
          (doall
           (many
            #(let [sexp `(* ~(rand) ~(rand-int 100))]
               (every? (fn [result] (is (= (eval sexp) result)))
                       (-> (cluster/eval-each sexp (set (drop 2 (cluster/members))))
                           (.get)))))))
        (testing "on all members"
          (doall
           (many
            #(let [sexp `(* ~(rand) ~(rand-int 100))]
               (every?
                (fn [result] (is (= (eval sexp) result)))
                (-> (cluster/eval-each sexp (cluster/members))
                    (.get))))))))
      (testing "with a distributed map")
      (testing "with a distributed mmap")
      (testing "with a distributed set"
        (let [colors ["red" "green" "blue"]
              set (cluster/get-set "colors")]
          (doseq [color colors] (-> set (.add color)))
          (testing "should only ever contain one of each entry"
            (doseq [color colors] (-> set (.add color)))
            (is (= (count colors) (count (cluster/get-set "colors")))))
          (testing "should be uniform across all members"
            (every?
             (fn [result] (is (= (count colors) result)))
             (-> (cluster/eval-each
                  `(do (require '[apparatus.cluster :as cluster])
                       (count (cluster/get-set "colors")))
                  (cluster/members))
                 (.get))))))
      (testing "with a distributed list")
      (testing "with a distributed queue")
      (testing "with a distributed topic")
      (testing "with a distributed lock")
      (testing "with a distributed transaction")
      (cluster/shutdown))))
