package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS events (observation_id INTEGER PRIMARY KEY, event_id TEXT, state TEXT, end_time DATETIME)`); err != nil {
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
	if _, err := db.Exec(`INSERT INTO events (observation_id, event_id, state, end_time) VALUES (1, '2026-02-11', 'ended', '2026-02-12T10:30:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events (observation_id, event_id, state, end_time) VALUES (2, '2026-02-11', 'dormant', NULL)`); err != nil {
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
}
