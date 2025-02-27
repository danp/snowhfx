package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

func main() {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var dbPath string
	fs.StringVar(&dbPath, "db", "data.db", "database file path")
	fs.Parse(os.Args[1:])

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?_pragma=journal_mode=WAL&_pragma=foreign_keys=ON&_pragma=busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events (observation_id INTEGER PRIMARY KEY REFERENCES observations (id), event_id TEXT, state TEXT, update_time DATETIME, end_time DATETIME, service_update TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	halifax, err := time.LoadLocation("America/Halifax")
	if err != nil {
		log.Fatal(err)
	}

	q := `WITH changes AS (SELECT id, t, content_id, LAG(content_id) OVER (ORDER BY t) AS prev_content_id FROM observations) SELECT changes.id, t, content->'updateTime'->>'txt', content->'serviceUpdate'->>'txt', content->'endTime'->>'txt' FROM changes JOIN contents ON contents.id=content_id WHERE content_id != prev_content_id OR prev_content_id IS NULL ORDER BY t`

	rows, err := db.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	s := state{s: stateDormant}

	for rows.Next() {
		var o observation
		if err := rows.Scan(&o.ID, &o.Time, &o.UpdateTime, &o.ServiceUpdate, &o.EndTime); err != nil {
			log.Fatal(err)
		}
		o.Time = o.Time.In(halifax)

		updateTime, ok := parseUpdateTime(o.UpdateTime, o.Time)
		if !ok {
			log.Fatalf("failed to parse update time: %q", o.UpdateTime)
		}

		// Feb. 6 | 11 p.m.7
		if before, ok := strings.CutSuffix(o.EndTime, "p.m.7"); ok {
			o.EndTime = before + "p.m."
		}
		endTime, ok := parseUpdateTime(o.EndTime, o.Time)
		if !ok {
			log.Fatalf("failed to parse end time: %q", o.EndTime)
		}

		newState := state{
			o:          o,
			eventID:    s.eventID,
			updateTime: updateTime,
			endTime:    endTime,
		}

		if !endTime.IsZero() {
			newState.s = stateEnded
		} else if !updateTime.IsZero() {
			newState.s = stateActive
		} else {
			newState.s = stateDormant
		}

		if s.s == stateDormant && newState.s == stateDormant {
			s = newState
			continue
		}

		dormantNew := s.s == stateDormant && (newState.s == stateActive || newState.s == stateEnded)
		endedNew := s.s == stateEnded && newState.s == stateActive
		endChange := s.s == stateEnded && newState.s == stateEnded && !endTime.Equal(s.endTime)

		if dormantNew || endedNew || endChange {
			eventTime := updateTime
			if eventTime.IsZero() {
				eventTime = endTime
			}
			newState.eventID = eventTime.Format("2006-01-02")
		}

		updateTimeSQL := sql.NullTime{Time: newState.updateTime.UTC(), Valid: !newState.updateTime.IsZero()}
		endTimeSQL := sql.NullTime{Time: newState.endTime.UTC(), Valid: !newState.endTime.IsZero()}
		serviceUpdateSQL := sql.NullString{String: newState.o.ServiceUpdate}
		if serviceUpdateSQL.String != "" && serviceUpdateSQL.String != "N/A" && serviceUpdateSQL.String != "N\\A" {
			serviceUpdateSQL.Valid = true
		}

		_, err = db.Exec(
			`INSERT INTO events (observation_id, event_id, state, update_time, end_time, service_update) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
			newState.o.ID,
			newState.eventID,
			newState.s.String(),
			updateTimeSQL,
			endTimeSQL,
			serviceUpdateSQL,
		)
		if err != nil {
			log.Fatal(err)
		}

		s = newState
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

type weatherEventState int

const (
	stateDormant weatherEventState = 1
	stateActive  weatherEventState = 2
	stateEnded   weatherEventState = 3
)

func (s weatherEventState) String() string {
	switch s {
	case stateDormant:
		return "dormant"
	case stateActive:
		return "active"
	case stateEnded:
		return "ended"
	default:
		return "unknown"
	}
}

type state struct {
	o          observation
	s          weatherEventState
	eventID    string
	updateTime time.Time
	endTime    time.Time
}

type observation struct {
	ID            int
	Time          time.Time
	UpdateTime    string
	ServiceUpdate string
	EndTime       string
}

var (
	squeezeRe = regexp.MustCompile(`\s+`)
	noonRe    = regexp.MustCompile(`(?i)\bnoon\b`)
)

func parseUpdateTime(txt string, t time.Time) (_ time.Time, ok bool) {
	txt = strings.TrimSpace(txt)
	if txt == "" || strings.EqualFold(txt, "N/A") || strings.EqualFold(txt, "N\\A") {
		return time.Time{}, true
	}

	txt = strings.ReplaceAll(txt, "a.m.", "AM")
	txt = strings.ReplaceAll(txt, "p.m.", "PM")
	txt = strings.ReplaceAll(txt, "|", " ")
	txt = strings.ReplaceAll(txt, "-", " ")
	txt = strings.ReplaceAll(txt, ".", " ")
	txt = strings.ReplaceAll(txt, ",", " ")
	txt = strings.ReplaceAll(txt, " at ", " ")
	txt = squeezeRe.ReplaceAllString(txt, " ")
	txt = noonRe.ReplaceAllString(txt, "12 PM")
	txt = strings.TrimSpace(txt)

	type format struct {
		s       string
		hasYear bool
		hasDate bool
		hasTime bool
	}
	formats := []format{
		{"1/2/2006 3:04 PM", true, true, true},
		{"3 PM Jan 2", false, true, true},
		{"3 PM January 2", false, true, true},
		{"3 PM Mon Jan 2", false, true, true},
		{"3 PM Mon January 2", false, true, true},
		{"3 PM Monday Jan 2", false, true, true},
		{"3 PM Monday January 2", false, true, true},
		{"3 PM", false, false, true},
		{"3:04 PM Jan 2", false, true, true},
		{"3:04 PM January 2", false, true, true},
		{"3:04 PM Mon Jan 2", false, true, true},
		{"3:04 PM Mon January 2", false, true, true},
		{"3:04 PM Monday Jan 2", false, true, true},
		{"3:04 PM Monday January 2", false, true, true},
		{"3:04 PM", false, false, true},
		{"Jan 2 3 PM", false, true, true},
		{"Jan 2 3:04 PM", false, true, true},
		{"January 2 3 PM", false, true, true},
		{"January 2 3:04 PM", false, true, true},
		{"Mon Jan 2 3 PM", false, true, true},
		{"Mon Jan 2 3:04 PM", false, true, true},
		{"Mon January 2 3 PM", false, true, true},
		{"Mon January 2 3:04 PM", false, true, true},
		{"Monday Jan 2", false, true, false},
		{"Monday Jan 2 3 PM", false, true, true},
		{"Monday Jan 2 3:04 PM", false, true, true},
		{"Monday January 2 3 PM", false, true, true},
		{"Monday January 2 3:04 PM", false, true, true},
	}

	for {
		for _, format := range formats {
			if parsed, err := time.ParseInLocation(format.s, txt, t.Location()); err == nil {
				if !format.hasYear {
					parsed = parsed.AddDate(t.Year(), 0, 0)
					if parsed.After(t) {
						parsed = parsed.AddDate(-1, 0, 0)
					}
				}
				if !format.hasDate {
					parsed = parsed.AddDate(0, int(t.Month())-1, t.Day())
					if parsed.After(t) {
						parsed = parsed.AddDate(0, 0, -1)
					}
				}
				if !format.hasTime {
					hms := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second
					parsed = parsed.Add(hms)
					if parsed.After(t) {
						parsed = parsed.AddDate(0, 0, -1)
					}
				}
				return parsed, true
			}
		}
		lastSpace := strings.LastIndex(txt, " ")
		if lastSpace == -1 {
			break
		}
		txt = txt[:lastSpace]
	}

	// If no format matches, return zero time
	return time.Time{}, false
}
