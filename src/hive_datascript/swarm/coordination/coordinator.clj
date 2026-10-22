;; SPDX-License-Identifier: MIT
;; Moved from hive-mcp.swarm.datascript.coordination.coordinator.

(ns hive-datascript.swarm.coordination.coordinator
  (:require [datascript.core :as d]
            [hive-datascript.swarm.connection :as conn]
            [hive-datascript.swarm.schema :as schema]
            [taoensso.timbre :as log]
            [malli.core :as m]))

(declare register-coordinator! update-heartbeat! get-coordinator get-all-coordinators get-coordinators-by-status get-coordinators-for-project mark-coordinator-terminated! mark-coordinator-stale! remove-coordinator!)

(defn register-coordinator!
  "Register a new coordinator instance.

   Arguments:
     coordinator-id - Unique identifier (required)
     opts           - Map with optional keys:
                      :project    - Project identifier
                      :pid        - OS process ID (default: current JVM PID)
                      :session-id - Session UUID (default: auto-generated)

   Returns:
     Transaction report with :tempids"

  [coordinator-id {:keys [project pid session-id]}]
  {:pre [(string? coordinator-id)]}
  (let [c (conn/ensure-conn)
        current-pid (or pid (.pid (java.lang.ProcessHandle/current)))
        session (or session-id (str (java.util.UUID/randomUUID)))
        tx-data {:coordinator/id coordinator-id
                 :coordinator/project project
                 :coordinator/pid current-pid
                 :coordinator/session-id session
                 :coordinator/started-at (conn/now)
                 :coordinator/heartbeat-at (conn/now)
                 :coordinator/status :active}]
    (log/info "Registering coordinator:" coordinator-id "project:" project "pid:" current-pid)
    (d/transact! c [tx-data])))

(defn update-heartbeat!
  "Update a coordinator's heartbeat timestamp.
   Also ensures status is :active (reactivates stale coordinators).

   Arguments:
     coordinator-id - Coordinator to update

   Returns:
     Transaction report or nil if coordinator not found"

  [coordinator-id]
  (let [c (conn/ensure-conn)
        db @c]
    (when-let [eid (:db/id (d/entity db [:coordinator/id coordinator-id]))]
      (log/trace "Heartbeat for coordinator:" coordinator-id)
      (d/transact! c [{:db/id eid
                       :coordinator/heartbeat-at (conn/now)
                       :coordinator/status :active}]))))

(defn get-coordinator
  "Get a coordinator by ID.

   Returns:
     Map with coordinator attributes or nil if not found"
  [coordinator-id]
  (let [c (conn/ensure-conn)
        db @c]
    (when-let [e (d/entity db [:coordinator/id coordinator-id])]
      (-> (into {} e)
          (dissoc :db/id)))))

(defn get-all-coordinators
  "Get all coordinators.

   Returns:
     Seq of maps with coordinator attributes"
  []
  (let [c (conn/ensure-conn)
        db @c
        eids (d/q '[:find [?e ...]
                    :where [?e :coordinator/id _]]
                  db)]
    (->> eids
         (map #(d/entity db %))
         (map (fn [e]
                (-> (into {} e)
                    (dissoc :db/id)))))))

(defn get-coordinators-by-status
  "Get coordinators filtered by status.

   Arguments:
     status - Status to filter by (:active :stale :terminated)

   Returns:
     Seq of coordinator maps"
  [status]
  {:pre [(contains? schema/coordinator-statuses status)]}
  (let [c (conn/ensure-conn)
        db @c
        eids (d/q '[:find [?e ...]
                    :in $ ?status
                    :where
                    [?e :coordinator/id _]
                    [?e :coordinator/status ?status]]
                  db status)]
    (->> eids
         (map #(d/entity db %))
         (map (fn [e]
                (-> (into {} e)
                    (dissoc :db/id)))))))

(defn get-coordinators-for-project
  "Get all coordinators for a specific project.

   Arguments:
     project - Project identifier

   Returns:
     Seq of coordinator maps"
  [project]
  (let [c (conn/ensure-conn)
        db @c
        eids (d/q '[:find [?e ...]
                    :in $ ?project
                    :where
                    [?e :coordinator/id _]
                    [?e :coordinator/project ?project]]
                  db project)]
    (->> eids
         (map #(d/entity db %))
         (map (fn [e]
                (-> (into {} e)
                    (dissoc :db/id)))))))

(defn mark-coordinator-terminated!
  "Mark a coordinator as terminated (graceful shutdown).

   Arguments:
     coordinator-id - Coordinator to mark

   Returns:
     Transaction report or nil if coordinator not found"
  [coordinator-id]
  (let [c (conn/ensure-conn)
        db @c]
    (when-let [eid (:db/id (d/entity db [:coordinator/id coordinator-id]))]
      (log/info "Marking coordinator terminated:" coordinator-id)
      (d/transact! c [{:db/id eid
                       :coordinator/status :terminated}]))))

(defn mark-coordinator-stale!
  "Mark a coordinator as stale (not responding).

   Arguments:
     coordinator-id - Coordinator to mark

   Returns:
     Transaction report or nil if coordinator not found"
  [coordinator-id]
  (let [c (conn/ensure-conn)
        db @c]
    (when-let [eid (:db/id (d/entity db [:coordinator/id coordinator-id]))]
      (log/warn "Marking coordinator stale:" coordinator-id)
      (d/transact! c [{:db/id eid
                       :coordinator/status :stale}]))))

(defn remove-coordinator!
  "Remove a coordinator entity.
   Should only be used for cleanup after graceful termination.

   Arguments:
     coordinator-id - Coordinator to remove

   Returns:
     Transaction report or nil if coordinator not found"
  [coordinator-id]
  (let [c (conn/ensure-conn)
        db @c]
    (when-let [eid (:db/id (d/entity db [:coordinator/id coordinator-id]))]
      (log/info "Removing coordinator:" coordinator-id)
      (d/transact! c [[:db/retractEntity eid]]))))

;;; =============================================================================
;;; Malli Contracts
;;; =============================================================================

(def CoordinatorStatus
  "Mirrors schema/coordinator-statuses."
  (into [:enum] (sort schema/coordinator-statuses)))

(def CoordinatorMap
  "A coordinator row projected out of DataScript (no :db/id)."
  [:map
   [:coordinator/id :string]
   [:coordinator/status CoordinatorStatus]
   [:coordinator/project {:optional true} [:maybe :string]]
   [:coordinator/pid {:optional true} int?]
   [:coordinator/session-id {:optional true} :string]
   [:coordinator/started-at {:optional true} inst?]
   [:coordinator/heartbeat-at {:optional true} inst?]])

(def RegisterOpts
  [:maybe
   [:map
    [:project {:optional true} [:maybe :string]]
    [:pid {:optional true} [:maybe int?]]
    [:session-id {:optional true} [:maybe :string]]]])

(def TxReport
  "datascript transaction report."
  [:map [:db-before :any] [:db-after :any] [:tx-data :any] [:tempids :any]])

(m/=> register-coordinator! [:=> [:cat :string RegisterOpts] TxReport])
(m/=> update-heartbeat! [:=> [:cat :string] [:maybe TxReport]])
(m/=> get-coordinator [:=> [:cat :string] [:maybe CoordinatorMap]])
(m/=> get-all-coordinators [:=> [:cat] [:maybe [:sequential CoordinatorMap]]])
(m/=> get-coordinators-by-status [:=> [:cat CoordinatorStatus] [:maybe [:sequential CoordinatorMap]]])
(m/=> get-coordinators-for-project [:=> [:cat [:maybe :string]] [:maybe [:sequential CoordinatorMap]]])
(m/=> mark-coordinator-terminated! [:=> [:cat :string] [:maybe TxReport]])
(m/=> mark-coordinator-stale! [:=> [:cat :string] [:maybe TxReport]])
(m/=> remove-coordinator! [:=> [:cat :string] [:maybe TxReport]])
