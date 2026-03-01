package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

const reportTTL = 72 * time.Hour

var allowedConditions = map[string]struct{}{
	"snow_covered": {},
	"cleared":      {},
	"icy":          {},
	"salted":       {},
	"needs_salt":   {},
}

type createReportRequest struct {
	SegmentKey     string   `json:"segment_key"`
	DatasetMode    string   `json:"dataset_mode"`
	Title          string   `json:"title"`
	Priority       int      `json:"priority"`
	Route          string   `json:"route"`
	Conditions     []string `json:"conditions"`
	Notes          string   `json:"notes"`
	ReporterID     string   `json:"reporter_id"`
	ReporterSecret string   `json:"reporter_secret"`
}

type reportResponse struct {
	ID           string   `json:"id"`
	SegmentKey   string   `json:"segment_key"`
	DatasetMode  string   `json:"dataset_mode"`
	EventID      string   `json:"event_id,omitempty"`
	Title        string   `json:"title"`
	Priority     int      `json:"priority"`
	Route        string   `json:"route"`
	Conditions   []string `json:"conditions"`
	Notes        string   `json:"notes"`
	ReporterID   string   `json:"reporter_id"`
	CreatedAt    string   `json:"created_at"`
	ExpiresAt    string   `json:"expires_at,omitempty"`
	ExpiredAt    string   `json:"expired_at,omitempty"`
	ExpireReason string   `json:"expire_reason,omitempty"`
	Expired      bool     `json:"expired"`
}

type currentEventResponse struct {
	EventID       string `json:"event_id"`
	State         string `json:"state"`
	MaxEndTime    string `json:"max_end_time"`
	UpdateTime    string `json:"update_time,omitempty"`
	ServiceUpdate string `json:"service_update,omitempty"`
}

func main() {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var dbPath string
	var addr string
	fs.StringVar(&dbPath, "db", "data.db", "database file path")
	fs.StringVar(&addr, "addr", ":8080", "http listen address")
	fs.Parse(os.Args[1:])

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?_pragma=journal_mode=WAL&_pragma=foreign_keys=ON&_pragma=busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}
	// Keep a single SQLite connection so writes are serialized through one writer DB handle.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()

	if err := ensureSchema(db); err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      newAPIHandler(db),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("api server listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newAPIHandler(db *sql.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/current-event", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		handleCurrentEvent(db, w, r)
	})
	mux.HandleFunc("GET /api/v1/community-reports", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		handleListReports(db, w, r)
	})
	mux.HandleFunc("POST /api/v1/community-reports", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		handleCreateReport(db, w, r)
	})
	return mux
}

func ensureSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS reporters (
			reporter_id TEXT PRIMARY KEY,
			secret_salt BLOB NOT NULL,
			secret_hash BLOB NOT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS community_reports (
			id TEXT PRIMARY KEY,
			segment_key TEXT NOT NULL,
			dataset_mode TEXT NOT NULL,
			event_id TEXT NOT NULL,
			title TEXT NOT NULL,
			priority INTEGER NOT NULL,
			route TEXT NOT NULL,
			conditions_json TEXT NOT NULL,
			notes TEXT NOT NULL,
			reporter_id TEXT NOT NULL REFERENCES reporters (reporter_id),
			created_at DATETIME NOT NULL,
			expires_at DATETIME,
			expired_at DATETIME,
			expire_reason TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS reports_meta (
			meta_key TEXT PRIMARY KEY,
			meta_value TEXT NOT NULL,
			updated_at DATETIME NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_community_reports_event_dataset_created
		   ON community_reports (event_id, dataset_mode, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_community_reports_segment
		   ON community_reports (segment_key)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_community_reports_active_dataset_created
		ON community_reports (dataset_mode, expired_at, expires_at, created_at DESC)`); err != nil {
		return err
	}
	return nil
}

func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func handleCurrentEvent(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	row, err := currentEvent(ctx, db)
	if err != nil {
		http.Error(w, fmt.Sprintf("query failed: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func handleListReports(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	q := r.URL.Query()
	datasetMode := strings.TrimSpace(q.Get("dataset_mode"))
	includeExpired := q.Get("include_expired") == "1"

	if datasetMode == "" {
		http.Error(w, "dataset_mode is required", http.StatusBadRequest)
		return
	}

	if err := applyExpiryPolicies(ctx, db); err != nil {
		http.Error(w, fmt.Sprintf("unable to apply expiry policies: %v", err), http.StatusInternalServerError)
		return
	}

	sqlQuery := `SELECT id, segment_key, dataset_mode, event_id, title, priority, route, conditions_json, notes, reporter_id, created_at, expires_at, expired_at, expire_reason
	   FROM community_reports
	  WHERE dataset_mode = ?`
	args := []any{datasetMode}
	if !includeExpired {
		sqlQuery += `
	    AND expired_at IS NULL
	    AND (expires_at IS NULL OR expires_at > ?)`
		args = append(args, time.Now().UTC().Format(time.RFC3339))
	}
	sqlQuery += `
	  ORDER BY created_at DESC`

	rows, err := db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf("query failed: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	reports := make([]reportResponse, 0, 64)
	reportedSet := map[string]struct{}{}
	for rows.Next() {
		var rr reportResponse
		var conditionsJSON string
		var expiresAt sql.NullString
		var expiredAt sql.NullString
		var expireReason sql.NullString
		if err := rows.Scan(
			&rr.ID,
			&rr.SegmentKey,
			&rr.DatasetMode,
			&rr.EventID,
			&rr.Title,
			&rr.Priority,
			&rr.Route,
			&conditionsJSON,
			&rr.Notes,
			&rr.ReporterID,
			&rr.CreatedAt,
			&expiresAt,
			&expiredAt,
			&expireReason,
		); err != nil {
			http.Error(w, fmt.Sprintf("scan failed: %v", err), http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal([]byte(conditionsJSON), &rr.Conditions); err != nil {
			http.Error(w, fmt.Sprintf("conditions decode failed: %v", err), http.StatusInternalServerError)
			return
		}
		if expiresAt.Valid {
			rr.ExpiresAt = expiresAt.String
		}
		if expiredAt.Valid {
			rr.ExpiredAt = expiredAt.String
			rr.Expired = true
		}
		if expireReason.Valid {
			rr.ExpireReason = expireReason.String
		}
		reports = append(reports, rr)
		reportedSet[rr.SegmentKey] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		http.Error(w, fmt.Sprintf("rows failed: %v", err), http.StatusInternalServerError)
		return
	}

	reportedKeys := make([]string, 0, len(reportedSet))
	for key := range reportedSet {
		reportedKeys = append(reportedKeys, key)
	}
	sort.Strings(reportedKeys)

	writeJSON(w, http.StatusOK, map[string]any{
		"reports":               reports,
		"reported_segment_keys": reportedKeys,
	})
}

func handleCreateReport(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var req createReportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if err := validateCreateRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := applyExpiryPolicies(ctx, db); err != nil {
		http.Error(w, fmt.Sprintf("unable to apply expiry policies: %v", err), http.StatusInternalServerError)
		return
	}

	conditionsJSON, err := json.Marshal(req.Conditions)
	if err != nil {
		http.Error(w, "unable to encode conditions", http.StatusBadRequest)
		return
	}

	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339)
	expiresAt := nowTime.Add(reportTTL).Format(time.RFC3339)
	reportID, err := randomID("rep_", 16)
	if err != nil {
		http.Error(w, "unable to generate report id", http.StatusInternalServerError)
		return
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		http.Error(w, "unable to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	if err := ensureReporterSecret(ctx, tx, req.ReporterID, req.ReporterSecret); err != nil {
		if errors.Is(err, errReporterSecretMismatch) {
			http.Error(w, "reporter_secret does not match reporter_id", http.StatusForbidden)
			return
		}
		http.Error(w, fmt.Sprintf("unable to verify reporter: %v", err), http.StatusInternalServerError)
		return
	}

	eventID, err := latestEventID(ctx, tx)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to resolve current event: %v", err), http.StatusInternalServerError)
		return
	}
	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO community_reports
		 (id, segment_key, dataset_mode, event_id, title, priority, route, conditions_json, notes, reporter_id, created_at, expires_at, expired_at, expire_reason)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)`,
		reportID,
		req.SegmentKey,
		req.DatasetMode,
		eventID,
		req.Title,
		req.Priority,
		req.Route,
		string(conditionsJSON),
		req.Notes,
		req.ReporterID,
		now,
		expiresAt,
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("insert report failed: %v", err), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit failed", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"report": reportResponse{
			ID:           reportID,
			SegmentKey:   req.SegmentKey,
			DatasetMode:  req.DatasetMode,
			EventID:      eventID,
			Title:        req.Title,
			Priority:     req.Priority,
			Route:        req.Route,
			Conditions:   req.Conditions,
			Notes:        req.Notes,
			ReporterID:   req.ReporterID,
			CreatedAt:    now,
			ExpiresAt:    expiresAt,
			ExpiredAt:    "",
			ExpireReason: "",
			Expired:      false,
		},
	})
}

func validateCreateRequest(req createReportRequest) error {
	if strings.TrimSpace(req.SegmentKey) == "" {
		return errors.New("segment_key is required")
	}
	if strings.TrimSpace(req.DatasetMode) == "" {
		return errors.New("dataset_mode is required")
	}
	if strings.TrimSpace(req.Title) == "" {
		return errors.New("title is required")
	}
	if strings.TrimSpace(req.ReporterID) == "" {
		return errors.New("reporter_id is required")
	}
	if strings.TrimSpace(req.ReporterSecret) == "" {
		return errors.New("reporter_secret is required")
	}
	if len(req.Notes) > 2000 {
		return errors.New("notes too long")
	}
	for _, condition := range req.Conditions {
		if _, ok := allowedConditions[condition]; !ok {
			return fmt.Errorf("invalid condition: %s", condition)
		}
	}
	return nil
}

func currentEvent(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (currentEventResponse, error) {
	var eventID sql.NullString
	var state sql.NullString
	var maxEndTime sql.NullString
	var updateTime sql.NullString
	var serviceUpdate sql.NullString
	err := q.QueryRowContext(ctx, `WITH latest AS (
  SELECT
    e.event_id,
    e.state,
    COALESCE(e.update_time, o.t) AS update_time,
    e.service_update
  FROM
    events e
    LEFT JOIN observations o ON o.id = e.observation_id
  ORDER BY
    e.observation_id DESC
  LIMIT
    1
)
SELECT
  latest.event_id AS event_id,
  latest.state AS state,
  latest.update_time AS update_time,
  latest.service_update AS service_update,
  (
    SELECT
      max(end_time)
    FROM
      events
  ) AS max_end_time
FROM
  latest`).Scan(&eventID, &state, &updateTime, &serviceUpdate, &maxEndTime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return currentEventResponse{}, nil
		}
		return currentEventResponse{}, err
	}
	res := currentEventResponse{}
	if eventID.Valid {
		res.EventID = eventID.String
	}
	if state.Valid {
		res.State = state.String
	}
	if maxEndTime.Valid {
		res.MaxEndTime = maxEndTime.String
	}
	if updateTime.Valid {
		res.UpdateTime = updateTime.String
	}
	if serviceUpdate.Valid {
		res.ServiceUpdate = serviceUpdate.String
	}
	return res, nil
}

func applyExpiryPolicies(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)

	_, err = tx.ExecContext(
		ctx,
		`UPDATE community_reports
		    SET expired_at = ?, expire_reason = COALESCE(expire_reason, 'ttl')
		  WHERE expired_at IS NULL
		    AND expires_at IS NOT NULL
		    AND expires_at <= ?`,
		now,
		now,
	)
	if err != nil {
		return err
	}

	currentEventID, err := latestEventID(ctx, tx)
	if err != nil {
		return err
	}
	previousEventID, err := metaValue(ctx, tx, "active_event_id")
	if err != nil {
		return err
	}

	if currentEventID != "" {
		if previousEventID == "" {
			if err := setMetaValue(ctx, tx, "active_event_id", currentEventID); err != nil {
				return err
			}
		} else if previousEventID != currentEventID {
			_, err = tx.ExecContext(
				ctx,
				`UPDATE community_reports
				    SET expired_at = ?, expire_reason = 'new_event'
				  WHERE expired_at IS NULL`,
				now,
			)
			if err != nil {
				return err
			}
			if err := setMetaValue(ctx, tx, "active_event_id", currentEventID); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func latestEventID(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (string, error) {
	var eventID sql.NullString
	err := q.QueryRowContext(
		ctx,
		`SELECT event_id
		   FROM events
		  WHERE event_id IS NOT NULL
		    AND TRIM(event_id) <> ''
		  ORDER BY observation_id DESC
		  LIMIT 1`,
	).Scan(&eventID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	if !eventID.Valid {
		return "", nil
	}
	return strings.TrimSpace(eventID.String), nil
}

func metaValue(ctx context.Context, tx *sql.Tx, key string) (string, error) {
	var value sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT meta_value FROM reports_meta WHERE meta_key = ?`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	if !value.Valid {
		return "", nil
	}
	return value.String, nil
}

func setMetaValue(ctx context.Context, tx *sql.Tx, key, value string) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO reports_meta (meta_key, meta_value, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(meta_key) DO UPDATE SET
		   meta_value = excluded.meta_value,
		   updated_at = excluded.updated_at`,
		key,
		value,
		time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

var errReporterSecretMismatch = errors.New("reporter secret mismatch")

func ensureReporterSecret(ctx context.Context, tx *sql.Tx, reporterID, secret string) error {
	var salt []byte
	var hash []byte
	err := tx.QueryRowContext(
		ctx,
		`SELECT secret_salt, secret_hash FROM reporters WHERE reporter_id = ?`,
		reporterID,
	).Scan(&salt, &hash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			newSalt := make([]byte, 32)
			if _, err := rand.Read(newSalt); err != nil {
				return err
			}
			newHash := hashSecret(newSalt, secret)
			now := time.Now().UTC().Format(time.RFC3339)
			_, err := tx.ExecContext(
				ctx,
				`INSERT INTO reporters (reporter_id, secret_salt, secret_hash, created_at, updated_at)
				 VALUES (?, ?, ?, ?, ?)`,
				reporterID,
				newSalt,
				newHash,
				now,
				now,
			)
			return err
		}
		return err
	}

	candidate := hashSecret(salt, secret)
	if subtle.ConstantTimeCompare(candidate, hash) != 1 {
		return errReporterSecretMismatch
	}

	_, err = tx.ExecContext(
		ctx,
		`UPDATE reporters SET updated_at = ? WHERE reporter_id = ?`,
		time.Now().UTC().Format(time.RFC3339),
		reporterID,
	)
	return err
}

func hashSecret(salt []byte, secret string) []byte {
	h := sha256.New()
	h.Write(salt)
	h.Write([]byte(secret))
	return h.Sum(nil)
}

func randomID(prefix string, byteLen int) (string, error) {
	raw := make([]byte, byteLen)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write JSON failed: %v", err)
	}
}
