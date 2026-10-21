;; SPDX-License-Identifier: MIT

(ns hive-datascript.swarm.slave-index
  "Slave rows, read once per connection and then kept current from each
   transaction's delta.

   Reading every slave entity is the expensive part of a roster query: the
   swarm keeps every row it ever saw, so a full read materializes thousands
   of entities to return the handful that are live. This index pays that
   cost once, then re-reads only the slave entities a transaction touched.

   Consumers that want to react instead of poll `subscribe!` and are called
   after any transaction that changed a slave row."
  (:require [datascript.core :as d]
            [taoensso.timbre :as log]))

(defn- entity->slave
  "Slave map for entity E, or nil when E no longer carries :slave/id."
  [e]
  (when (:slave/id e)
    (-> (into {} e)
        (dissoc :db/id)
        (update :slave/parent #(when % (:slave/id %)))
        (update :slave/current-task #(when % (:task/id %))))))

(defn scan
  "{eid slave-map} for every slave row in DB. The full read."
  [db]
  (into {}
        (keep (fn [eid] (when-let [s (entity->slave (d/entity db eid))] [eid s])))
        (d/q '[:find [?e ...] :where [?e :slave/id _]] db)))

(defn touched-eids
  "Entity ids whose slave attributes TX-DATA changed."
  [tx-data]
  (into #{} (comp (filter #(= "slave" (namespace (:a %)))) (map :e)) tx-data))

(defn apply-delta
  "BY-EID advanced by one transaction: only the TOUCHED slave entities are
   re-read from DB-AFTER."
  [by-eid touched db-after]
  (reduce (fn [acc eid]
            (if-let [s (entity->slave (d/entity db-after eid))]
              (assoc acc eid s)
              (dissoc acc eid)))
          by-eid
          touched))

;; {:conn c :db db :by-eid {eid slave}}. Valid only while :conn and :db are
;; identical to the live ones: a replaced conn or a transaction the listener
;; did not see falls back to a rescan instead of serving stale rows.
(defonce ^:private index (atom nil))

;; {key (fn [])}: called after a transaction that changed a slave row.
(defonce ^:private subscribers (atom {}))

(defn- notify! []
  (doseq [[k f] @subscribers]
    (try (f)
         (catch Throwable t
           (log/warn t "slave-index subscriber failed" k)))))

(defn- on-tx!
  "Advance the index by one transaction on conn C. A transaction that touches
   no slave row only moves the basis forward, so it never forces a rescan."
  [c {:keys [db-before db-after tx-data]}]
  (let [touched (touched-eids tx-data)]
    (swap! index
           (fn [idx]
             (if (and (identical? c (:conn idx)) (identical? db-before (:db idx)))
               {:conn c :db db-after :by-eid (cond-> (:by-eid idx)
                                               (seq touched) (apply-delta touched db-after))}
               idx)))
    (when (seq touched)
      (notify!))))

(defn- rebuild! [c db]
  (d/listen! c ::index (fn [report] (on-tx! c report)))
  (:by-eid (reset! index {:conn c :db db :by-eid (scan db)})))

(defn slaves
  "{eid slave-map} for conn C, whose current value is DB. Served from the
   index when it is current; rebuilt, with its listener installed on C,
   when it is not."
  [c db]
  (let [idx @index]
    (if (and (identical? c (:conn idx)) (identical? db (:db idx)))
      (:by-eid idx)
      (rebuild! c db))))

(defn subscribe!
  "Call F (no args) after every transaction that changes a slave row.
   Re-subscribing under KEY replaces the previous F. Returns KEY."
  [key f]
  (swap! subscribers assoc key f)
  key)

(defn unsubscribe!
  "Stop calling the subscriber under KEY."
  [key]
  (swap! subscribers dissoc key)
  nil)
