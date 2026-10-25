package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func insertWeatherObservation(t *testing.T, db *sql.DB, at time.Time, temperature any) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO weather_observations
		(observed_at, station_code, temperature_c, condition_text, raw_json, fetched_at)
		VALUES (?, 'test', ?, 'Rain', '{}', ?)`, at.Format(time.RFC3339), temperature, at.Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadRecentObservationsPreservesMissingTemperature(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	now := time.Now().UTC()
	insertWeatherObservation(t, db, now.Add(-time.Hour), nil)
	insertWeatherObservation(t, db, now, 0)
	rows, err := loadRecentObservations(context.Background(), db, "test", 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d observations; want 2", len(rows))
	}
	if rows[0].TemperatureC.Valid {
		t.Fatal("missing temperature became a valid reading")
	}
	if !rows[1].TemperatureC.Valid || rows[1].TemperatureC.Float64 != 0 {
		t.Fatalf("real zero temperature was lost: %+v", rows[1].TemperatureC)
	}
}

func TestDeriveSurfaceConditionsDataQuality(t *testing.T) {
	now := time.Date(2026, time.March, 1, 14, 30, 0, 0, time.UTC)
	observation := func(age time.Duration, temp float64) weatherObservationRow {
		return weatherObservationRow{
			ObservedAt:   now.Add(-age).Format(time.RFC3339Nano),
			TemperatureC: sql.NullFloat64{Float64: temp, Valid: true},
		}
	}
	missing := observation(30*time.Minute, 0)
	missing.TemperatureC.Valid = false
	missing.ConditionText = "Rain"
	zero := observation(30*time.Minute, 0)
	zero.ConditionText = "Rain"
	invalid := observation(0, 0)
	invalid.ObservedAt = "invalid"
	oldSnow := observation(13*time.Hour, 10)
	oldSnow.ConditionText = "Snow"
	dailies := []weatherDailyRow{{SnowOnGroundCM: sql.NullFloat64{Float64: 20, Valid: true}}}
	for _, tc := range []struct {
		name         string
		observations []weatherObservationRow
		wantStatus   string
		wantTrend    any
	}{
		{"no observations", nil, "unavailable", nil},
		{"missing latest temperature", []weatherObservationRow{observation(time.Hour, 4), missing}, "unavailable", nil},
		{"stale latest observation", []weatherObservationRow{observation(2*time.Hour+time.Second, 0)}, "unavailable", nil},
		{"invalid observation timestamp", []weatherObservationRow{invalid}, "unavailable", nil},
		{"future observation", []weatherObservationRow{observation(-time.Hour, 0)}, "unavailable", nil},
		{"real zero temperature", []weatherObservationRow{zero}, "slushy", nil},
		{"missing older temperature", []weatherObservationRow{missing, observation(0, 0.5)}, "melting_wet", nil},
		{"ignore old snow and temperature", []weatherObservationRow{oldSnow, observation(0, 0.5)}, "melting_wet", nil},
		{"recent falling temperatures", []weatherObservationRow{observation(time.Hour, 4), observation(0, 0.5)}, "flash_refreeze_risk", -3.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveSurfaceConditions(now, "test", tc.observations, dailies)
			if got.Status != tc.wantStatus {
				t.Fatalf("status = %s; want %s", got.Status, tc.wantStatus)
			}
			if got.Detail["temp_trend_window_c"] != tc.wantTrend {
				t.Errorf("temperature trend = %v; want %v", got.Detail["temp_trend_window_c"], tc.wantTrend)
			}
			if got.Status == "unavailable" && (got.Score != 0 || len(got.Tags) != 0) {
				t.Errorf("unavailable data still has condition signals: %+v", got)
			}
			if tc.name == "missing latest temperature" && got.Detail["temperature_c"] != nil {
				t.Error("missing temperature should be null in response detail")
			}
		})
	}
}

func TestSurfaceConditionsRejectStaleStoredSignal(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	oldNow := time.Now().UTC().Add(-3 * time.Hour)
	insertWeatherObservation(t, db, oldNow, 0)
	wr := &weatherRefresher{db: db, stationCode: "test", now: func() time.Time { return oldNow }}
	if err := wr.recomputeSurface(context.Background()); err != nil {
		t.Fatal(err)
	}
	var storedStatus string
	if err := db.QueryRow(`SELECT status FROM surface_conditions WHERE id = 1`).Scan(&storedStatus); err != nil {
		t.Fatal(err)
	}
	if storedStatus != "slushy" {
		t.Fatalf("expected initially valid slushy signal; got %s", storedStatus)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/surface-conditions", nil)
	w := httptest.NewRecorder()
	newAPIHandler(db, nil).ServeHTTP(w, req)
	var got surfaceConditionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || got.Status != "unavailable" || got.Score != 0 || len(got.Tags) != 0 {
		t.Fatalf("stale conditions were served as current: status=%d response=%+v", w.Code, got)
	}
}

func TestRecomputeSurfaceWithoutObservations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	wr := &weatherRefresher{db: db, stationCode: "test", now: time.Now}
	if err := wr.recomputeSurface(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := currentSurfaceConditions(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "unavailable" {
		t.Fatalf("expected unavailable without observations; got %s", got.Status)
	}
}
