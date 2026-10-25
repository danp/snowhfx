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
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

const (
	reportTTL             = 72 * time.Hour
	defaultWeatherStation = "yhz"
	defaultWeatherClimate = "8202251"
	maxObservationAge     = 2 * time.Hour
	observationWindow     = 12 * time.Hour
)

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

type weatherObservationRow struct {
	ObservedAt    string
	TemperatureC  sql.NullFloat64
	ConditionText string
	Precip1hMM    float64
	HasPrecip1h   bool
}

type weatherDailyRow struct {
	ClimateDate    string
	MinTempC       sql.NullFloat64
	MaxTempC       sql.NullFloat64
	TotalRainMM    sql.NullFloat64
	TotalSnowCM    sql.NullFloat64
	TotalPrecipMM  sql.NullFloat64
	SnowOnGroundCM sql.NullFloat64
	MaxGustKPH     sql.NullFloat64
}

type surfaceConditionsResponse struct {
	ComputedAt  string         `json:"computed_at"`
	StationCode string         `json:"station_code"`
	Status      string         `json:"status"`
	Score       int            `json:"score"`
	Tags        []string       `json:"tags"`
	Summary     string         `json:"summary"`
	Detail      map[string]any `json:"detail"`
}

type weatherRefresher struct {
	db              *sql.DB
	client          *http.Client
	stationCode     string
	observationURL  string
	dailyClimateID  string
	refreshInterval time.Duration
	dailyLookback   int
	now             func() time.Time
}

type httpStatusError struct {
	StatusCode int
	URL        string
	Body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("unexpected status %d from %s: %s", e.StatusCode, e.URL, e.Body)
}

func main() {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var dbPath string
	var addr string
	var featuresDir string
	var weatherEnabled bool
	var weatherStation string
	var weatherObservationURL string
	var weatherClimateID string
	var weatherRefreshInterval time.Duration
	fs.StringVar(&dbPath, "db", "data.db", "database file path")
	fs.StringVar(&addr, "addr", ":8080", "http listen address")
	fs.StringVar(&featuresDir, "features-dir", "", "persistent features directory (default $XDG_DATA_HOME/snowhfx/features or ~/.local/share/snowhfx/features)")
	fs.BoolVar(&weatherEnabled, "weather-enabled", true, "enable periodic weather ingestion")
	fs.StringVar(&weatherStation, "weather-station", defaultWeatherStation, "Environment Canada station code")
	fs.StringVar(&weatherObservationURL, "weather-observation-url", "", "override observation endpoint URL")
	fs.StringVar(&weatherClimateID, "weather-climate-id", defaultWeatherClimate, "Environment Canada climate-daily station identifier")
	fs.DurationVar(&weatherRefreshInterval, "weather-refresh-interval", 15*time.Minute, "weather refresh interval")
	fs.Parse(os.Args[1:])
	if featuresDir == "" {
		var err error
		featuresDir, err = defaultFeaturesDir()
		if err != nil {
			log.Fatal(err)
		}
	}
	features, err := newFeatureStore(featuresDir, bundledFeatures)
	if err != nil {
		log.Fatal(err)
	}
	defer features.Close()
	log.Printf("feature data stored in %s", featuresDir)

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

	if weatherEnabled {
		refresher := &weatherRefresher{
			db:              db,
			client:          &http.Client{Timeout: 10 * time.Second},
			stationCode:     weatherStation,
			observationURL:  weatherObservationURL,
			dailyClimateID:  weatherClimateID,
			refreshInterval: weatherRefreshInterval,
			dailyLookback:   7,
			now:             time.Now,
		}
		go refresher.Start(context.Background())
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      newAPIHandler(db, features),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("api server listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newAPIHandler(db *sql.DB, features *featureStore) http.Handler {
	mux := http.NewServeMux()
	if features != nil {
		features.registerRoutes(mux)
	}
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
	mux.HandleFunc("OPTIONS /api/v1/community-reports", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /api/v1/surface-conditions", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		handleSurfaceConditions(db, w, r)
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
		`CREATE TABLE IF NOT EXISTS weather_observations (
			observed_at DATETIME PRIMARY KEY,
			station_code TEXT NOT NULL,
			temperature_c REAL,
			dew_point_c REAL,
			humidity_pct INTEGER,
			wind_kph REAL,
			wind_gust_kph REAL,
			visibility_km REAL,
			pressure_kpa REAL,
			condition_code TEXT,
			condition_text TEXT,
			precip_1h_mm REAL,
			raw_json TEXT NOT NULL,
			fetched_at DATETIME NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS weather_daily (
			station_code TEXT NOT NULL,
			climate_date TEXT NOT NULL,
			min_temp_c REAL,
			max_temp_c REAL,
			total_rain_mm REAL,
			total_snow_cm REAL,
			total_precip_mm REAL,
			snow_on_ground_cm REAL,
			max_gust_kph REAL,
			raw_json TEXT NOT NULL,
			fetched_at DATETIME NOT NULL,
			PRIMARY KEY (station_code, climate_date)
		)`,
		`CREATE TABLE IF NOT EXISTS surface_conditions (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			station_code TEXT NOT NULL,
			computed_at DATETIME NOT NULL,
			status TEXT NOT NULL,
			score INTEGER NOT NULL,
			tags_json TEXT NOT NULL,
			summary TEXT NOT NULL,
			detail_json TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_community_reports_event_dataset_created
		   ON community_reports (event_id, dataset_mode, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_community_reports_segment
		   ON community_reports (segment_key)`,
		`CREATE INDEX IF NOT EXISTS idx_weather_observations_station_observed
		   ON weather_observations (station_code, observed_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_weather_daily_station_date
		   ON weather_daily (station_code, climate_date DESC)`,
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

func handleSurfaceConditions(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	row, err := currentSurfaceConditions(ctx, db)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "surface conditions unavailable", http.StatusNotFound)
			return
		}
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

func currentSurfaceConditions(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (surfaceConditionsResponse, error) {
	var res surfaceConditionsResponse
	var tagsJSON string
	var detailJSON string
	err := q.QueryRowContext(
		ctx,
		`SELECT computed_at, station_code, status, score, tags_json, summary, detail_json
		   FROM surface_conditions
		  WHERE id = 1`,
	).Scan(&res.ComputedAt, &res.StationCode, &res.Status, &res.Score, &tagsJSON, &res.Summary, &detailJSON)
	if err != nil {
		return surfaceConditionsResponse{}, err
	}
	if err := json.Unmarshal([]byte(tagsJSON), &res.Tags); err != nil {
		return surfaceConditionsResponse{}, err
	}
	if err := json.Unmarshal([]byte(detailJSON), &res.Detail); err != nil {
		return surfaceConditionsResponse{}, err
	}
	observedAt, _ := res.Detail["latest_observed_at"].(string)
	if !observationIsRecent(time.Now().UTC(), observedAt, maxObservationAge) {
		res = unavailableSurface(res, "Recent weather observations are unavailable.")
	} else if res.Detail["temperature_c"] == nil {
		res = unavailableSurface(res, "The latest weather observation has no temperature reading.")
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

func (wr *weatherRefresher) Start(ctx context.Context) {
	if wr.client == nil {
		wr.client = &http.Client{Timeout: 10 * time.Second}
	}
	if wr.now == nil {
		wr.now = time.Now
	}
	if wr.refreshInterval <= 0 {
		wr.refreshInterval = 15 * time.Minute
	}
	if wr.dailyLookback <= 0 {
		wr.dailyLookback = 7
	}
	if err := wr.refresh(ctx); err != nil {
		log.Printf("weather refresh failed: %v", err)
	}

	ticker := time.NewTicker(wr.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := wr.refresh(ctx); err != nil {
				log.Printf("weather refresh failed: %v", err)
			}
		}
	}
}

func (wr *weatherRefresher) refresh(ctx context.Context) error {
	if err := wr.fetchObservation(ctx); err != nil {
		return err
	}
	if wr.dailyClimateID != "" {
		for _, offset := range []int{0, -1} {
			day := wr.now().UTC().AddDate(0, 0, offset)
			if err := wr.fetchDaily(ctx, day); err != nil {
				return err
			}
		}
	}
	return wr.recomputeSurface(ctx)
}

func (wr *weatherRefresher) fetchObservation(ctx context.Context) error {
	url := wr.observationURL
	if strings.TrimSpace(url) == "" {
		url = fmt.Sprintf("https://weather.gc.ca/api/app/en/Observation?codes[]=%s", wr.stationCode)
	}

	payload, raw, err := fetchJSON(ctx, wr.client, url)
	if err != nil {
		return err
	}

	record, err := firstRecord(payload)
	if err != nil {
		return err
	}
	observedAt := firstString(record, "timeStamp", "timestamp", "observation_time", "observed_at", "observationTime", "dateTime", "date_time", "datetime", "date")
	if observedAt == "" {
		return errors.New("observation payload missing timestamp")
	}
	temperature, hasTemperature := firstFloat(record, "temperature", "temperature_c", "temp", "air_temperature")
	dewPoint, hasDewPoint := firstFloat(record, "dewpoint", "dew_point", "dew_point_c", "dewpoint_c")
	humidity, hasHumidity := firstInt(record, "humidity", "humidity_pct", "relative_humidity")
	windKPH, hasWindKPH := firstFloat(record, "windSpeed", "wind", "wind_kph", "wind_speed")
	windGustKPH, hasWindGustKPH := firstFloat(record, "windGust", "wind_gust", "wind_gust_kph", "gust", "gust_kph")
	visibilityKM, hasVisibilityKM := firstFloat(record, "visibility", "visibility_km", "visUnround")
	pressureKPA, hasPressureKPA := firstFloat(record, "pressure", "pressure_kpa")
	conditionCode := firstString(record, "iconCode", "condition_code", "weather_code", "icon_code")
	conditionText := firstString(record, "condition", "condition_text", "weather", "weather_condition", "wx_condition", "text_summary")
	precip1h, hasPrecip1h := firstFloat(record, "precip_1h_mm", "precip1h", "precipitation_1h", "precipitation")

	var temperatureValue any
	if hasTemperature {
		temperatureValue = temperature
	}
	var humidityValue any
	if hasHumidity {
		humidityValue = humidity
	}
	var precipValue any
	if hasPrecip1h {
		precipValue = precip1h
	}

	_, err = wr.db.ExecContext(
		ctx,
		`INSERT INTO weather_observations
		 (observed_at, station_code, temperature_c, dew_point_c, humidity_pct, wind_kph, wind_gust_kph, visibility_km, pressure_kpa, condition_code, condition_text, precip_1h_mm, raw_json, fetched_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(observed_at) DO UPDATE SET
		   station_code = excluded.station_code,
		   temperature_c = excluded.temperature_c,
		   dew_point_c = excluded.dew_point_c,
		   humidity_pct = excluded.humidity_pct,
		   wind_kph = excluded.wind_kph,
		   wind_gust_kph = excluded.wind_gust_kph,
		   visibility_km = excluded.visibility_km,
		   pressure_kpa = excluded.pressure_kpa,
		   condition_code = excluded.condition_code,
		   condition_text = excluded.condition_text,
		   precip_1h_mm = excluded.precip_1h_mm,
		   raw_json = excluded.raw_json,
		   fetched_at = excluded.fetched_at`,
		observedAt,
		wr.stationCode,
		temperatureValue,
		valueOrNil(hasDewPoint, dewPoint),
		humidityValue,
		valueOrNil(hasWindKPH, windKPH),
		valueOrNil(hasWindGustKPH, windGustKPH),
		valueOrNil(hasVisibilityKM, visibilityKM),
		valueOrNil(hasPressureKPA, pressureKPA),
		emptyToNil(conditionCode),
		emptyToNil(conditionText),
		precipValue,
		string(raw),
		wr.now().UTC().Format(time.RFC3339),
	)
	return err
}

func (wr *weatherRefresher) fetchDaily(ctx context.Context, day time.Time) error {
	lookback := wr.dailyLookback
	if lookback <= 0 {
		lookback = 1
	}

	var payload any
	var raw []byte
	var err error
	for offset := 0; offset < lookback; offset++ {
		candidate := day.AddDate(0, 0, -offset)
		url := fmt.Sprintf("https://api.weather.gc.ca/collections/climate-daily/items/%s.%d.%d.%d?f=json", wr.dailyClimateID, candidate.Year(), int(candidate.Month()), candidate.Day())
		payload, raw, err = fetchJSON(ctx, wr.client, url)
		if err == nil {
			break
		}
		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
			continue
		}
		return err
	}
	if err != nil {
		return err
	}

	record, err := firstRecord(payload)
	if err != nil {
		return err
	}
	climateDate := firstString(record, "LOCAL_DATE", "climate_date", "date")
	if climateDate == "" {
		climateDate = day.Format("2006-01-02")
	} else if len(climateDate) >= len("2006-01-02") {
		climateDate = climateDate[:len("2006-01-02")]
	}
	minTemp, hasMinTemp := firstFloat(record, "MIN_TEMPERATURE", "min_temp_c", "min_temperature")
	maxTemp, hasMaxTemp := firstFloat(record, "MAX_TEMPERATURE", "max_temp_c", "max_temperature")
	totalRain, hasTotalRain := firstFloat(record, "TOTAL_RAIN", "total_rain_mm")
	totalSnow, hasTotalSnow := firstFloat(record, "TOTAL_SNOW", "total_snow_cm")
	totalPrecip, hasTotalPrecip := firstFloat(record, "TOTAL_PRECIPITATION", "total_precip_mm")
	snowOnGround, hasSnowOnGround := firstFloat(record, "SNOW_ON_GROUND", "snow_on_ground_cm")
	maxGust, hasMaxGust := firstFloat(record, "SPEED_MAX_GUST", "MAX_GUST_SPEED", "max_gust_kph")

	_, err = wr.db.ExecContext(
		ctx,
		`INSERT INTO weather_daily
		 (station_code, climate_date, min_temp_c, max_temp_c, total_rain_mm, total_snow_cm, total_precip_mm, snow_on_ground_cm, max_gust_kph, raw_json, fetched_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(station_code, climate_date) DO UPDATE SET
		   min_temp_c = excluded.min_temp_c,
		   max_temp_c = excluded.max_temp_c,
		   total_rain_mm = excluded.total_rain_mm,
		   total_snow_cm = excluded.total_snow_cm,
		   total_precip_mm = excluded.total_precip_mm,
		   snow_on_ground_cm = excluded.snow_on_ground_cm,
		   max_gust_kph = excluded.max_gust_kph,
		   raw_json = excluded.raw_json,
		   fetched_at = excluded.fetched_at`,
		wr.stationCode,
		climateDate,
		valueOrNil(hasMinTemp, minTemp),
		valueOrNil(hasMaxTemp, maxTemp),
		valueOrNil(hasTotalRain, totalRain),
		valueOrNil(hasTotalSnow, totalSnow),
		valueOrNil(hasTotalPrecip, totalPrecip),
		valueOrNil(hasSnowOnGround, snowOnGround),
		valueOrNil(hasMaxGust, maxGust),
		string(raw),
		wr.now().UTC().Format(time.RFC3339),
	)
	return err
}

func (wr *weatherRefresher) recomputeSurface(ctx context.Context) error {
	observations, err := loadRecentObservations(ctx, wr.db, wr.stationCode, 12)
	if err != nil {
		return err
	}
	dailies, err := loadRecentDaily(ctx, wr.db, wr.stationCode, 2)
	if err != nil {
		return err
	}

	signal := deriveSurfaceConditions(wr.now().UTC(), wr.stationCode, observations, dailies)
	tagsJSON, err := json.Marshal(signal.Tags)
	if err != nil {
		return err
	}
	detailJSON, err := json.Marshal(signal.Detail)
	if err != nil {
		return err
	}

	_, err = wr.db.ExecContext(
		ctx,
		`INSERT INTO surface_conditions
		 (id, station_code, computed_at, status, score, tags_json, summary, detail_json)
		 VALUES (1, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   station_code = excluded.station_code,
		   computed_at = excluded.computed_at,
		   status = excluded.status,
		   score = excluded.score,
		   tags_json = excluded.tags_json,
		   summary = excluded.summary,
		   detail_json = excluded.detail_json`,
		signal.StationCode,
		signal.ComputedAt,
		signal.Status,
		signal.Score,
		string(tagsJSON),
		signal.Summary,
		string(detailJSON),
	)
	return err
}

func loadRecentObservations(ctx context.Context, db *sql.DB, stationCode string, limit int) ([]weatherObservationRow, error) {
	rows, err := db.QueryContext(
		ctx,
		`SELECT observed_at, temperature_c, COALESCE(condition_text, ''), precip_1h_mm
		   FROM weather_observations
		  WHERE station_code = ?
		  ORDER BY observed_at DESC
		  LIMIT ?`,
		stationCode,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]weatherObservationRow, 0, limit)
	for rows.Next() {
		var row weatherObservationRow
		var precip sql.NullFloat64
		if err := rows.Scan(&row.ObservedAt, &row.TemperatureC, &row.ConditionText, &precip); err != nil {
			return nil, err
		}
		if precip.Valid {
			row.Precip1hMM = precip.Float64
			row.HasPrecip1h = true
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result, nil
}

func loadRecentDaily(ctx context.Context, db *sql.DB, stationCode string, limit int) ([]weatherDailyRow, error) {
	rows, err := db.QueryContext(
		ctx,
		`SELECT climate_date, min_temp_c, max_temp_c, total_rain_mm, total_snow_cm, total_precip_mm, snow_on_ground_cm, max_gust_kph
		   FROM weather_daily
		  WHERE station_code = ?
		  ORDER BY climate_date DESC
		  LIMIT ?`,
		stationCode,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]weatherDailyRow, 0, limit)
	for rows.Next() {
		var row weatherDailyRow
		if err := rows.Scan(
			&row.ClimateDate,
			&row.MinTempC,
			&row.MaxTempC,
			&row.TotalRainMM,
			&row.TotalSnowCM,
			&row.TotalPrecipMM,
			&row.SnowOnGroundCM,
			&row.MaxGustKPH,
		); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func observationIsRecent(now time.Time, timestamp string, maxAge time.Duration) bool {
	observedAt, err := time.Parse(time.RFC3339Nano, timestamp)
	return err == nil && !observedAt.Before(now.Add(-maxAge)) && !observedAt.After(now.Add(5*time.Minute))
}

func unavailableSurface(res surfaceConditionsResponse, summary string) surfaceConditionsResponse {
	res.Status = "unavailable"
	res.Score = 0
	res.Tags = []string{}
	res.Summary = summary
	return res
}

func deriveSurfaceConditions(now time.Time, stationCode string, observations []weatherObservationRow, dailies []weatherDailyRow) surfaceConditionsResponse {
	res := surfaceConditionsResponse{
		ComputedAt:  now.Format(time.RFC3339),
		StationCode: stationCode,
		Detail: map[string]any{
			"observation_count":   len(observations),
			"temperature_c":       nil,
			"temp_trend_window_c": nil,
			"derived_at":          now.Format(time.RFC3339),
		},
	}
	if len(observations) == 0 {
		return unavailableSurface(res, "Recent weather observations are unavailable.")
	}
	latest := observations[len(observations)-1]
	res.Detail["latest_observed_at"] = latest.ObservedAt
	res.Detail["condition_text"] = latest.ConditionText
	res.Detail["temperature_c"] = valueOrNil(latest.TemperatureC.Valid, latest.TemperatureC.Float64)
	if !observationIsRecent(now, latest.ObservedAt, maxObservationAge) {
		return unavailableSurface(res, "Recent weather observations are unavailable.")
	}
	if !latest.TemperatureC.Valid {
		return unavailableSurface(res, "The latest weather observation has no temperature reading.")
	}
	temperature := latest.TemperatureC.Float64
	var earliestTemperature float64
	temperatureCount := 0
	recentCount := 0
	tempsAboveZero := 0
	hadPrecip := false
	sawSnowText := false
	sawRainText := false
	for _, obs := range observations {
		if !observationIsRecent(now, obs.ObservedAt, observationWindow) {
			continue
		}
		recentCount++
		if obs.TemperatureC.Valid {
			if temperatureCount == 0 {
				earliestTemperature = obs.TemperatureC.Float64
			}
			temperatureCount++
			if obs.TemperatureC.Float64 > 0 {
				tempsAboveZero++
			}
		}
		if obs.HasPrecip1h && obs.Precip1hMM > 0 {
			hadPrecip = true
		}
		condition := strings.ToLower(obs.ConditionText)
		if strings.Contains(condition, "snow") || strings.Contains(condition, "ice pellets") {
			sawSnowText = true
		}
		if strings.Contains(condition, "rain") || strings.Contains(condition, "drizzle") {
			sawRainText = true
		}
	}

	snowpackCM := 0.0
	snowpackKnown := false
	dayMaxTemp := 0.0
	dayMinTemp := 0.0
	if len(dailies) > 0 {
		for _, daily := range dailies {
			if daily.SnowOnGroundCM.Valid && daily.SnowOnGroundCM.Float64 > snowpackCM {
				snowpackCM = daily.SnowOnGroundCM.Float64
				snowpackKnown = true
			}
		}
		if dailies[0].MaxTempC.Valid {
			dayMaxTemp = dailies[0].MaxTempC.Float64
		}
		if dailies[0].MinTempC.Valid {
			dayMinTemp = dailies[0].MinTempC.Float64
		}
	}

	tempTrend := temperature - earliestTemperature
	trendKnown := temperatureCount >= 2
	nearFreezing := temperature >= -1 && temperature <= 2
	meltingWet := snowpackKnown && snowpackCM > 0 && temperature > 0 && tempsAboveZero >= 1
	slushy := nearFreezing && (hadPrecip || sawSnowText || sawRainText)
	flashRefreeze := meltingWet && temperature <= 1 && trendKnown && tempTrend < 0
	icyLikely := temperature <= 0 && (meltingWet || (snowpackKnown && snowpackCM > 0 && trendKnown && tempTrend < 0))
	packedSnow := snowpackKnown && snowpackCM > 0 && temperature < 0 && !hadPrecip && !meltingWet
	freshSnow := sawSnowText

	tags := make([]string, 0, 6)
	score := 0
	if freshSnow {
		tags = append(tags, "fresh_snow")
		score += 2
	}
	if slushy {
		tags = append(tags, "slushy")
		score += 2
	}
	if meltingWet {
		tags = append(tags, "melting_wet")
		score += 3
	}
	if flashRefreeze {
		tags = append(tags, "flash_refreeze_risk")
		score += 4
	}
	if icyLikely {
		tags = append(tags, "icy_likely")
		score += 3
	}
	if packedSnow {
		tags = append(tags, "packed_snow_likely")
		score += 2
	}
	if len(tags) == 0 {
		tags = append(tags, "clear_or_drying")
	}

	status := "clear_or_drying"
	summary := "No strong snow-surface signal detected."
	switch {
	case flashRefreeze:
		status = "flash_refreeze_risk"
		summary = "Snowpack is melting now; wet surfaces may freeze as temperatures fall."
	case icyLikely:
		status = "icy_likely"
		summary = "Recent thaw and falling temperatures suggest icy spots are likely."
	case meltingWet:
		status = "melting_wet"
		summary = "Snow on the ground is thawing and likely leaving standing water or runoff."
	case slushy:
		status = "slushy"
		summary = "Near-freezing precipitation suggests slushy surfaces."
	case freshSnow:
		status = "fresh_snow"
		summary = "Recent snowfall likely leaves fresh accumulation on untreated surfaces."
	case packedSnow:
		status = "packed_snow_likely"
		summary = "Cold temperatures with existing snowpack suggest packed snow remains."
	}

	detail := map[string]any{
		"observation_count":      recentCount,
		"latest_observed_at":     latest.ObservedAt,
		"temperature_c":          temperature,
		"condition_text":         latest.ConditionText,
		"temp_trend_window_c":    valueOrNil(trendKnown, tempTrend),
		"temps_above_zero_count": tempsAboveZero,
		"snow_on_ground_cm":      valueOrZero(snowpackKnown, snowpackCM),
		"snowpack_known":         snowpackKnown,
		"daily_max_temp_c":       valueOrZero(len(dailies) > 0 && dailies[0].MaxTempC.Valid, dayMaxTemp),
		"daily_min_temp_c":       valueOrZero(len(dailies) > 0 && dailies[0].MinTempC.Valid, dayMinTemp),
		"derived_at":             now.Format(time.RFC3339),
	}

	return surfaceConditionsResponse{
		ComputedAt:  now.Format(time.RFC3339),
		StationCode: stationCode,
		Status:      status,
		Score:       score,
		Tags:        tags,
		Summary:     summary,
		Detail:      detail,
	}
}

func fetchJSON(ctx context.Context, client *http.Client, url string) (any, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return nil, nil, &httpStatusError{
			StatusCode: res.StatusCode,
			URL:        url,
			Body:       strings.TrimSpace(string(body)),
		}
	}
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, err
	}
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, nil, err
	}
	return payload, raw, nil
}

func firstRecord(payload any) (map[string]any, error) {
	switch t := payload.(type) {
	case []any:
		for _, item := range t {
			if rec := unwrapRecord(item); rec != nil {
				return rec, nil
			}
		}
	case map[string]any:
		for _, key := range []string{"observations", "items", "data", "results", "features"} {
			value, ok := t[key]
			if !ok {
				continue
			}
			switch items := value.(type) {
			case []any:
				for _, item := range items {
					if rec := unwrapRecord(item); rec != nil {
						return rec, nil
					}
				}
			case map[string]any:
				if rec := unwrapRecord(items); rec != nil {
					return rec, nil
				}
			}
		}
		if rec := unwrapRecord(t); rec != nil {
			return rec, nil
		}
	}
	return nil, errors.New("no record found in weather payload")
}

func unwrapRecord(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if properties, ok := m["properties"].(map[string]any); ok {
		return properties
	}
	for _, key := range []string{"observation", "item"} {
		if nested, ok := m[key].(map[string]any); ok {
			return nested
		}
	}
	return m
}

func firstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			if s, ok := valueAsString(value); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func firstFloat(m map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			if f, ok := valueAsFloat(value); ok {
				return f, true
			}
		}
	}
	return 0, false
}

func firstInt(m map[string]any, keys ...string) (int, bool) {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			if i, ok := valueAsInt(value); ok {
				return i, true
			}
		}
	}
	return 0, false
}

func valueAsString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	case json.Number:
		return t.String(), true
	case map[string]any:
		for _, key := range []string{"value", "text", "metric", "english", "label", "en"} {
			if nested, ok := t[key]; ok {
				if s, ok := valueAsString(nested); ok {
					return s, true
				}
			}
		}
	case []any:
		if len(t) > 0 {
			return valueAsString(t[0])
		}
	}
	return "", false
}

func valueAsFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	case map[string]any:
		for _, key := range []string{"value", "metric", "celsius", "amount"} {
			if nested, ok := t[key]; ok {
				if f, ok := valueAsFloat(nested); ok {
					return f, true
				}
			}
		}
	case []any:
		if len(t) > 0 {
			return valueAsFloat(t[0])
		}
	}
	return 0, false
}

func valueAsInt(v any) (int, bool) {
	if f, ok := valueAsFloat(v); ok {
		return int(f), true
	}
	return 0, false
}

func emptyToNil(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.TrimSpace(s)
}

func valueOrNil(ok bool, v float64) any {
	if !ok {
		return nil
	}
	return v
}

func valueOrZero(ok bool, v float64) any {
	if !ok {
		return nil
	}
	return v
}
