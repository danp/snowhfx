package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS observations (id INTEGER PRIMARY KEY, t DATETIME, content_id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS events (observation_id INTEGER PRIMARY KEY, event_id TEXT, state TEXT, update_time DATETIME, end_time DATETIME, service_update TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := ensureSchema(db); err != nil {
		t.Fatal(err)
	}
	return db
}

func insertEvent(t *testing.T, db *sql.DB, obs int, eventID string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO events (observation_id, event_id, state) VALUES (?, ?, ?)`, obs, eventID, "dormant"); err != nil {
		t.Fatal(err)
	}
}

func setupTestServer(t *testing.T, db *sql.DB) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(newAPIHandler(db))
	t.Cleanup(ts.Close)
	return ts
}

func doJSON(t *testing.T, ts *httptest.Server, method, path string, body any) (int, []byte) {
	t.Helper()

	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}

	req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, resBody
}

func TestCommunityReportsPreflight(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/community-reports", nil)
	req.Header.Set("Origin", "https://snowhfx.danp.net")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "content-type")
	w := httptest.NewRecorder()
	newAPIHandler(nil).ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("unexpected allowed origin: %q", got)
	}
	if got := w.Header().Get("Access-Control-Allow-Methods"); !contains(strings.Split(got, ", "), http.MethodPost) {
		t.Errorf("POST missing from allowed methods: %q", got)
	}
	if got := w.Header().Get("Access-Control-Allow-Headers"); !strings.EqualFold(got, "Content-Type") {
		t.Errorf("unexpected allowed headers: %q", got)
	}
	if w.Body.Len() != 0 {
		t.Errorf("expected empty body, got %q", w.Body.String())
	}
}

func TestCreateAndListReports(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	insertEvent(t, db, 1, "2026-02-11")

	ts := setupTestServer(t, db)
	createReq := createReportRequest{
		SegmentKey:     "stable-1",
		DatasetMode:    "cycling",
		Title:          "Test Segment",
		Priority:       2,
		Route:          "Route A",
		Conditions:     []string{"icy"},
		Notes:          "note",
		ReporterID:     "r-1",
		ReporterSecret: "secret-1",
	}

	createCode, createBody := doJSON(t, ts, http.MethodPost, "/api/v1/community-reports", createReq)
	if createCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", createCode, string(createBody))
	}

	listCode, listBody := doJSON(t, ts, http.MethodGet, "/api/v1/community-reports?dataset_mode=cycling", nil)
	if listCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", listCode, string(listBody))
	}
	var payload struct {
		Reports []reportResponse `json:"reports"`
	}
	if err := json.Unmarshal(listBody, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(payload.Reports))
	}
	if payload.Reports[0].EventID != "2026-02-11" {
		t.Fatalf("unexpected event_id: %q", payload.Reports[0].EventID)
	}
	if payload.Reports[0].Expired {
		t.Fatalf("newly created report should not be expired")
	}
}

func TestCreateRejectsReporterSecretMismatch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	insertEvent(t, db, 1, "2026-02-11")
	ts := setupTestServer(t, db)

	first := createReportRequest{
		SegmentKey:     "stable-1",
		DatasetMode:    "cycling",
		Title:          "A",
		Priority:       1,
		Route:          "R",
		Conditions:     []string{"cleared"},
		ReporterID:     "r-1",
		ReporterSecret: "secret-1",
	}
	res1Code, res1Body := doJSON(t, ts, http.MethodPost, "/api/v1/community-reports", first)
	if res1Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", res1Code, string(res1Body))
	}

	second := first
	second.SegmentKey = "stable-2"
	second.ReporterSecret = "different-secret"
	res2Code, res2Body := doJSON(t, ts, http.MethodPost, "/api/v1/community-reports", second)
	if res2Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", res2Code, string(res2Body))
	}
}

func TestTTLAutoExpiry(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	insertEvent(t, db, 1, "2026-02-11")
	ts := setupTestServer(t, db)

	if _, err := db.Exec(`INSERT INTO reporters (reporter_id, secret_salt, secret_hash, created_at, updated_at) VALUES ('r-ttl', X'00', X'00', ?, ?)`, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	if _, err := db.Exec(
		`INSERT INTO community_reports (id, segment_key, dataset_mode, event_id, title, priority, route, conditions_json, notes, reporter_id, created_at, expires_at, expired_at, expire_reason)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)`,
		"rep-ttl",
		"stable-ttl",
		"cycling",
		"2026-02-11",
		"TTL",
		1,
		"R",
		`["icy"]`,
		"",
		"r-ttl",
		time.Now().UTC().Add(-2*time.Hour).Format(time.RFC3339),
		past,
	); err != nil {
		t.Fatal(err)
	}

	listCode, listBody := doJSON(t, ts, http.MethodGet, "/api/v1/community-reports?dataset_mode=cycling", nil)
	if listCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", listCode, string(listBody))
	}
	var payload struct {
		Reports []reportResponse `json:"reports"`
	}
	if err := json.Unmarshal(listBody, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Reports) != 0 {
		t.Fatalf("expected 0 active reports, got %d", len(payload.Reports))
	}

	var expiredAt sql.NullString
	var reason sql.NullString
	if err := db.QueryRow(`SELECT expired_at, expire_reason FROM community_reports WHERE id = 'rep-ttl'`).Scan(&expiredAt, &reason); err != nil {
		t.Fatal(err)
	}
	if !expiredAt.Valid || expiredAt.String == "" {
		t.Fatalf("expected expired_at to be set")
	}
	if reason.String != "ttl" {
		t.Fatalf("expected expire_reason ttl, got %q", reason.String)
	}
}

func TestNewEventExpiresExistingReports(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	insertEvent(t, db, 1, "2026-02-11")
	ts := setupTestServer(t, db)

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO reporters (reporter_id, secret_salt, secret_hash, created_at, updated_at) VALUES ('r-ne', X'00', X'00', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO community_reports (id, segment_key, dataset_mode, event_id, title, priority, route, conditions_json, notes, reporter_id, created_at, expires_at, expired_at, expire_reason)
		 VALUES ('rep-ne', 'stable-ne', 'cycling', '2026-02-11', 'NE', 1, 'R', '["icy"]', '', 'r-ne', ?, ?, NULL, NULL)`,
		now,
		time.Now().UTC().Add(24*time.Hour).Format(time.RFC3339),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO reports_meta (meta_key, meta_value, updated_at) VALUES ('active_event_id', '2026-02-11', ?)`, now); err != nil {
		t.Fatal(err)
	}
	insertEvent(t, db, 2, "2026-02-12")

	if err := applyExpiryPolicies(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	var expiredAt sql.NullString
	var reason sql.NullString
	if err := db.QueryRow(`SELECT expired_at, expire_reason FROM community_reports WHERE id = 'rep-ne'`).Scan(&expiredAt, &reason); err != nil {
		t.Fatal(err)
	}
	if !expiredAt.Valid || expiredAt.String == "" {
		t.Fatalf("expected expired_at to be set")
	}
	if reason.String != "new_event" {
		t.Fatalf("expected expire_reason new_event, got %q", reason.String)
	}

	listCode, listBody := doJSON(t, ts, http.MethodGet, "/api/v1/community-reports?dataset_mode=cycling", nil)
	if listCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", listCode, string(listBody))
	}
	var payload struct {
		Reports []reportResponse `json:"reports"`
	}
	if err := json.Unmarshal(listBody, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Reports) != 0 {
		t.Fatalf("expected 0 active reports after event change, got %d", len(payload.Reports))
	}
}

func TestCurrentEventEndpoint(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO observations (id, t, content_id) VALUES (2, '2026-02-12T13:15:00Z', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events (observation_id, event_id, state, end_time) VALUES (1, '2026-02-11', 'ended', '2026-02-12T10:30:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events (observation_id, event_id, state, update_time, end_time, service_update) VALUES (2, '2026-02-11', 'dormant', NULL, NULL, 'Winter weather event underway; crews preparing.')`); err != nil {
		t.Fatal(err)
	}

	ts := setupTestServer(t, db)
	resCode, resBody := doJSON(t, ts, http.MethodGet, "/api/v1/current-event", nil)
	if resCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resCode, string(resBody))
	}
	var payload currentEventResponse
	if err := json.Unmarshal(resBody, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.EventID != "2026-02-11" {
		t.Fatalf("unexpected event_id: %q", payload.EventID)
	}
	if payload.State != "dormant" {
		t.Fatalf("unexpected state: %q", payload.State)
	}
	if payload.MaxEndTime != "2026-02-12T10:30:00Z" {
		t.Fatalf("unexpected max_end_time: %q", payload.MaxEndTime)
	}
	if payload.UpdateTime != "2026-02-12T13:15:00Z" {
		t.Fatalf("unexpected update_time: %q", payload.UpdateTime)
	}
	if payload.ServiceUpdate != "Winter weather event underway; crews preparing." {
		t.Fatalf("unexpected service_update: %q", payload.ServiceUpdate)
	}
}

func TestWeatherRefreshAndSurfaceConditionsEndpoint(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	if _, err := db.Exec(
		`INSERT INTO weather_observations (observed_at, station_code, temperature_c, condition_text, raw_json, fetched_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		"2026-03-01T09:00:00Z",
		"ahd",
		4.0,
		"Rain showers",
		`{}`,
		"2026-03-01T09:05:00Z",
	); err != nil {
		t.Fatal(err)
	}

	weatherTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/obs":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `[{"observedAt":"Halifax Dockyard","provinceCode":"ns","climateId":"8202240","tcid":"ahd","timeStamp":"2026-03-01T14:00:00.000Z","iconCode":"29","condition":"","temperature":{"metric":"0.5"},"dewpoint":{"metric":"0.1"},"pressure":{"metric":"101.2"},"visibility":{"metric":"20"},"humidity":"94","windSpeed":{"metric":"18"},"windGust":{"metric":""},"tcId":"AHD"}]`)
		case r.URL.Path == "/collections/climate-daily/items/8202250.2026.3.1":
			http.NotFound(w, r)
		case r.URL.Path == "/collections/climate-daily/items/8202250.2026.2.28":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"properties":{"LOCAL_DATE":"2026-02-28","MIN_TEMPERATURE":-6,"MAX_TEMPERATURE":1,"SNOW_ON_GROUND":20}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer weatherTS.Close()

	refresher := &weatherRefresher{
		db:             db,
		client:         weatherTS.Client(),
		stationCode:    "ahd",
		observationURL: weatherTS.URL + "/obs",
		dailyClimateID: "8202250",
		now: func() time.Time {
			return time.Date(2026, time.March, 1, 14, 30, 0, 0, time.UTC)
		},
		dailyLookback: 3,
	}

	originalTransport := refresher.client.Transport
	refresher.client.Transport = rewriteTransport{
		base:    originalTransport,
		baseURL: weatherTS.URL,
	}

	if err := refresher.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	ts := setupTestServer(t, db)
	resCode, resBody := doJSON(t, ts, http.MethodGet, "/api/v1/surface-conditions", nil)
	if resCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resCode, string(resBody))
	}

	var payload surfaceConditionsResponse
	if err := json.Unmarshal(resBody, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Status != "flash_refreeze_risk" {
		t.Fatalf("unexpected status: %q", payload.Status)
	}
	if !contains(payload.Tags, "melting_wet") {
		t.Fatalf("expected melting_wet tag, got %#v", payload.Tags)
	}
	if payload.Detail["snow_on_ground_cm"] == nil {
		t.Fatalf("expected snow_on_ground_cm detail")
	}

	var climateDate string
	if err := db.QueryRow(`SELECT climate_date FROM weather_daily WHERE station_code = ? ORDER BY climate_date DESC LIMIT 1`, "ahd").Scan(&climateDate); err != nil {
		t.Fatal(err)
	}
	if climateDate != "2026-02-28" {
		t.Fatalf("expected fallback daily climate_date 2026-02-28, got %q", climateDate)
	}
}

type rewriteTransport struct {
	base    http.RoundTripper
	baseURL string
}

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	if strings.HasPrefix(req.URL.Host, "api.weather.gc.ca") {
		replacement, err := http.NewRequestWithContext(req.Context(), req.Method, rt.baseURL+req.URL.Path, nil)
		if err != nil {
			return nil, err
		}
		cloned.URL = replacement.URL
	}
	if rt.base == nil {
		rt.base = http.DefaultTransport
	}
	return rt.base.RoundTrip(cloned)
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
