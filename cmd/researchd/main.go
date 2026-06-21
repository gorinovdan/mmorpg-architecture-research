package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	eventStream = "mmorpg:events"
	liveChannel = "mmorpg:events:live"
)

type app struct {
	db       *pgxpool.Pool
	redis    *redis.Client
	upgrader websocket.Upgrader
}

type commandRequest struct {
	MessageID string `json:"message_id"`
	PlayerID  string `json:"player_id"`
	Operation string `json:"operation"`
	Amount    int    `json:"amount"`
	Delivery  string `json:"delivery"`
	Fault     string `json:"fault"`
}

type sagaRequest struct {
	SagaID   string `json:"saga_id"`
	PlayerID string `json:"player_id"`
	ItemID   string `json:"item_id"`
	Price    int    `json:"price"`
}

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://mmorpg:mmorpg@localhost:15432/mmorpg?sslmode=disable"))
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:16379")})
	defer redisClient.Close()

	a := &app{
		db:    db,
		redis: redisClient,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}

	role := env("ROLE", "backend")
	switch role {
	case "backend":
		if err := a.migrate(ctx); err != nil {
			log.Fatalf("migrate: %v", err)
		}
		if err := a.runHTTP(); err != nil {
			log.Fatalf("http server: %v", err)
		}
	case "worker":
		a.waitForSchema(ctx)
		a.runWorker(ctx)
	default:
		log.Fatalf("unknown ROLE %q", role)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func (a *app) migrate(ctx context.Context) error {
	sql := `
create table if not exists players (
	id text primary key,
	gold integer not null default 0,
	version integer not null default 0,
	updated_at timestamptz not null default now()
);
create table if not exists inbox (
	message_id text primary key,
	processed_at timestamptz not null default now()
);
create table if not exists outbox (
	id bigserial primary key,
	event_id text not null unique,
	aggregate_id text not null,
	event_type text not null,
	payload jsonb not null,
	status text not null default 'pending',
	attempts integer not null default 0,
	created_at timestamptz not null default now(),
	published_at timestamptz
);
create table if not exists chat_messages (
	id bigserial primary key,
	room text not null,
	user_id text not null,
	message text not null,
	created_at timestamptz not null default now()
);
create table if not exists sagas (
	id text primary key,
	player_id text not null,
	item_id text not null,
	price integer not null,
	status text not null,
	step text not null,
	updated_at timestamptz not null default now()
);`
	_, err := a.db.Exec(ctx, sql)
	return err
}

func (a *app) waitForSchema(ctx context.Context) {
	for {
		var exists bool
		err := a.db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = 'outbox'
			)
		`).Scan(&exists)
		if err == nil && exists {
			return
		}
		log.Printf("waiting for database schema")
		time.Sleep(500 * time.Millisecond)
	}
}

func (a *app) runHTTP() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/admin/reset", a.handleReset)
	mux.HandleFunc("/command", a.handleCommand)
	mux.HandleFunc("/player", a.handlePlayer)
	mux.HandleFunc("/chat", a.handleChat)
	mux.HandleFunc("/saga/purchase", a.handleSagaPurchase)
	mux.HandleFunc("/ws", a.handleWS)
	mux.HandleFunc("/experiments/pubsub-loss", a.handlePubSubLoss)
	mux.HandleFunc("/experiments/cache-stale", a.handleCacheStale)
	mux.HandleFunc("/metrics/summary", a.handleMetrics)
	addr := env("HTTP_ADDR", ":8080")
	log.Printf("research backend listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	errDB := a.db.Ping(ctx)
	errRedis := a.redis.Ping(ctx).Err()
	writeJSON(w, map[string]any{
		"ok":       errDB == nil && errRedis == nil,
		"postgres": errDB == nil,
		"redis":    errRedis == nil,
	})
}

func (a *app) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()
	if _, err := a.db.Exec(ctx, `truncate table inbox, outbox, chat_messages, sagas, players restart identity`); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := a.redis.FlushDB(ctx).Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (a *app) handleCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	start := time.Now()
	var req commandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.MessageID == "" || req.PlayerID == "" {
		http.Error(w, "message_id and player_id are required", http.StatusBadRequest)
		return
	}
	if req.Amount == 0 {
		req.Amount = 1
	}
	if req.Operation == "" {
		req.Operation = "grant"
	}
	if req.Delivery == "" {
		req.Delivery = "outbox"
	}
	duplicate, gold, err := a.applyCommand(r.Context(), req)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errInsufficientGold) {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, map[string]any{
		"ok":         true,
		"duplicate":  duplicate,
		"player_id":  req.PlayerID,
		"gold":       gold,
		"latency_ms": float64(time.Since(start).Microseconds()) / 1000,
		"delivery":   req.Delivery,
	})
}

var errInsufficientGold = errors.New("insufficient gold")

func (a *app) applyCommand(ctx context.Context, req commandRequest) (bool, int, error) {
	tx, err := a.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `insert into inbox(message_id) values($1) on conflict do nothing`, req.MessageID)
	if err != nil {
		return false, 0, err
	}
	if tag.RowsAffected() == 0 {
		var gold int
		_ = tx.QueryRow(ctx, `select gold from players where id=$1`, req.PlayerID).Scan(&gold)
		return true, gold, tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `insert into players(id, gold) values($1, 0) on conflict do nothing`, req.PlayerID); err != nil {
		return false, 0, err
	}
	var current int
	if err := tx.QueryRow(ctx, `select gold from players where id=$1 for update`, req.PlayerID).Scan(&current); err != nil {
		return false, 0, err
	}
	next := current
	switch req.Operation {
	case "grant":
		next += req.Amount
	case "spend":
		if current < req.Amount {
			return false, current, errInsufficientGold
		}
		next -= req.Amount
	default:
		return false, current, fmt.Errorf("unsupported operation %q", req.Operation)
	}
	if _, err := tx.Exec(ctx, `update players set gold=$2, version=version+1, updated_at=now() where id=$1`, req.PlayerID, next); err != nil {
		return false, 0, err
	}
	if req.Delivery != "sync" {
		payload, _ := json.Marshal(map[string]any{
			"message_id": req.MessageID,
			"player_id":  req.PlayerID,
			"operation":  req.Operation,
			"amount":     req.Amount,
			"gold":       next,
			"fault":      req.Fault,
		})
		if _, err := tx.Exec(ctx, `
insert into outbox(event_id, aggregate_id, event_type, payload)
values($1, $2, $3, $4)`,
			req.MessageID+":event", req.PlayerID, "player."+req.Operation, payload); err != nil {
			return false, 0, err
		}
	}
	return false, next, tx.Commit(ctx)
}

func (a *app) handlePlayer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	playerID := r.URL.Query().Get("id")
	if playerID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	useCache := r.URL.Query().Get("cache") == "true"
	cacheKey := "player:" + playerID + ":gold"
	if useCache {
		if value, err := a.redis.Get(ctx, cacheKey).Result(); err == nil {
			gold, _ := strconv.Atoi(value)
			writeJSON(w, map[string]any{"player_id": playerID, "gold": gold, "source": "redis-cache"})
			return
		}
	}
	var gold int
	if err := a.db.QueryRow(ctx, `select gold from players where id=$1`, playerID).Scan(&gold); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if useCache {
		_ = a.redis.Set(ctx, cacheKey, strconv.Itoa(gold), 30*time.Second).Err()
	}
	writeJSON(w, map[string]any{"player_id": playerID, "gold": gold, "source": "postgres"})
}

func (a *app) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Room    string `json:"room"`
		UserID  string `json:"user_id"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Room == "" {
		req.Room = "global"
	}
	if req.UserID == "" || req.Message == "" {
		http.Error(w, "user_id and message are required", http.StatusBadRequest)
		return
	}
	var id int64
	if err := a.db.QueryRow(r.Context(), `
insert into chat_messages(room, user_id, message) values($1, $2, $3) returning id`,
		req.Room, req.UserID, req.Message).Scan(&id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	payload, _ := json.Marshal(map[string]any{"id": id, "room": req.Room, "user_id": req.UserID, "message": req.Message})
	_ = a.redis.Publish(r.Context(), "mmorpg:chat:"+req.Room, payload).Err()
	_ = a.redis.XAdd(r.Context(), &redis.XAddArgs{
		Stream: "mmorpg:chat-stream",
		Values: map[string]any{"event_id": fmt.Sprintf("chat:%d", id), "payload": string(payload)},
	}).Err()
	writeJSON(w, map[string]any{"ok": true, "chat_id": id})
}

func (a *app) handleSagaPurchase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req sagaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.SagaID == "" || req.PlayerID == "" || req.ItemID == "" {
		http.Error(w, "saga_id, player_id and item_id are required", http.StatusBadRequest)
		return
	}
	if req.Price == 0 {
		req.Price = 10
	}
	ctx := r.Context()
	if _, err := a.db.Exec(ctx, `insert into players(id, gold) values($1, 0) on conflict do nothing`, req.PlayerID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err := a.db.Exec(ctx, `
insert into sagas(id, player_id, item_id, price, status, step)
values($1, $2, $3, $4, 'pending', 'reserve')
on conflict (id) do nothing`,
		req.SagaID, req.PlayerID, req.ItemID, req.Price)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "saga_id": req.SagaID, "status": "pending"})
}

func (a *app) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	ctx := r.Context()
	last := r.URL.Query().Get("last")
	if last == "" {
		last = "0-0"
	}
	start := last
	if last != "0-0" {
		start = "(" + last
	}
	messages, err := a.redis.XRange(ctx, eventStream, start, "+").Result()
	if err == nil {
		for _, msg := range messages {
			_ = conn.WriteJSON(map[string]any{
				"id":       msg.ID,
				"event_id": msg.Values["event_id"],
				"payload":  msg.Values["payload"],
				"replay":   true,
			})
		}
	}
	pubsub := a.redis.Subscribe(ctx, liveChannel)
	defer pubsub.Close()
	ch := pubsub.Channel(redis.WithChannelSize(8))
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			if msg == nil {
				return
			}
			if err := conn.WriteJSON(map[string]any{"payload": msg.Payload, "replay": false}); err != nil {
				return
			}
		}
	}
}

func (a *app) handlePubSubLoss(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	channel := "mmorpg:pubsub-loss"
	payload := fmt.Sprintf("transient-%d", time.Now().UnixNano())
	if err := a.redis.Publish(ctx, channel, payload).Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pubsub := a.redis.Subscribe(ctx, channel)
	defer pubsub.Close()
	_, _ = pubsub.ReceiveTimeout(ctx, 250*time.Millisecond)
	select {
	case msg := <-pubsub.Channel():
		writeJSON(w, map[string]any{"delivered_to_late_subscriber": msg != nil && msg.Payload == payload})
	case <-time.After(250 * time.Millisecond):
		writeJSON(w, map[string]any{"delivered_to_late_subscriber": false})
	}
}

func (a *app) handleCacheStale(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	playerID := "cache-player"
	cacheKey := "player:" + playerID + ":gold"
	if _, err := a.db.Exec(ctx, `
insert into players(id, gold) values($1, 100)
on conflict (id) do update set gold=100, version=players.version+1`, playerID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := a.redis.Set(ctx, cacheKey, "100", time.Minute).Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := a.db.Exec(ctx, `update players set gold=150, version=version+1 where id=$1`, playerID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cached, _ := a.redis.Get(ctx, cacheKey).Int()
	var actual int
	if err := a.db.QueryRow(ctx, `select gold from players where id=$1`, playerID).Scan(&actual); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"cached_gold": cached, "actual_gold": actual, "stale": cached != actual})
}

func (a *app) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	summary, err := a.collectSummary(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, summary)
}

func (a *app) collectSummary(ctx context.Context) (map[string]any, error) {
	var outboxPending int
	var outboxMaxAge float64
	if err := a.db.QueryRow(ctx, `
select count(*), coalesce(extract(epoch from max(now() - created_at))*1000, 0)
from outbox where status='pending'`).Scan(&outboxPending, &outboxMaxAge); err != nil {
		return nil, err
	}
	var completed, compensated int
	if err := a.db.QueryRow(ctx, `select count(*) filter (where status='completed'), count(*) filter (where status='compensated') from sagas`).Scan(&completed, &compensated); err != nil {
		return nil, err
	}
	messages, _ := a.redis.XRange(ctx, eventStream, "-", "+").Result()
	seen := map[string]int{}
	for _, msg := range messages {
		if value, ok := msg.Values["event_id"].(string); ok {
			seen[value]++
		}
	}
	duplicates := 0
	for _, count := range seen {
		if count > 1 {
			duplicates += count - 1
		}
	}
	return map[string]any{
		"outbox_pending":               outboxPending,
		"outbox_max_age_ms":            outboxMaxAge,
		"redis_stream_len":             len(messages),
		"redis_unique_events":          len(seen),
		"redis_duplicate_events":       duplicates,
		"saga_completed":               completed,
		"saga_compensated":             compensated,
		"transport_duplicate_observed": duplicates > 0,
	}, nil
}

func (a *app) runWorker(ctx context.Context) {
	log.Printf("research worker started")
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.processOutbox(ctx); err != nil {
				log.Printf("outbox worker error: %v", err)
			}
			if err := a.processSagas(ctx); err != nil {
				log.Printf("saga worker error: %v", err)
			}
		}
	}
}

func (a *app) processOutbox(ctx context.Context) error {
	tx, err := a.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
select id, event_id, aggregate_id, event_type, payload::text, attempts
from outbox
where status='pending'
order by id
limit 20
for update skip locked`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type event struct {
		id          int64
		eventID     string
		aggregateID string
		eventType   string
		payload     string
		attempts    int
	}
	var events []event
	for rows.Next() {
		var item event
		if err := rows.Scan(&item.id, &item.eventID, &item.aggregateID, &item.eventType, &item.payload, &item.attempts); err != nil {
			return err
		}
		events = append(events, item)
	}
	for _, item := range events {
		if err := a.redis.XAdd(ctx, &redis.XAddArgs{
			Stream: eventStream,
			Values: map[string]any{
				"event_id":     item.eventID,
				"aggregate_id": item.aggregateID,
				"event_type":   item.eventType,
				"payload":      item.payload,
			},
		}).Err(); err != nil {
			return err
		}
		if err := a.redis.Publish(ctx, liveChannel, item.payload).Err(); err != nil {
			return err
		}
		if strings.Contains(item.payload, "publish_no_ack") && item.attempts == 0 {
			if _, err := tx.Exec(ctx, `update outbox set attempts=attempts+1 where id=$1`, item.id); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.Exec(ctx, `update outbox set status='published', attempts=attempts+1, published_at=now() where id=$1`, item.id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (a *app) processSagas(ctx context.Context) error {
	tx, err := a.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
select id, player_id, item_id, price
from sagas
where status='pending'
order by updated_at
limit 10
for update skip locked`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type saga struct {
		id       string
		playerID string
		itemID   string
		price    int
	}
	var sagas []saga
	for rows.Next() {
		var item saga
		if err := rows.Scan(&item.id, &item.playerID, &item.itemID, &item.price); err != nil {
			return err
		}
		sagas = append(sagas, item)
	}
	for _, item := range sagas {
		var gold int
		if err := tx.QueryRow(ctx, `select gold from players where id=$1 for update`, item.playerID).Scan(&gold); err != nil {
			return err
		}
		status := "compensated"
		step := "compensate"
		if gold >= item.price && item.itemID != "missing-item" {
			if _, err := tx.Exec(ctx, `update players set gold=gold-$2, version=version+1 where id=$1`, item.playerID, item.price); err != nil {
				return err
			}
			status = "completed"
			step = "commit"
		}
		if _, err := tx.Exec(ctx, `update sagas set status=$2, step=$3, updated_at=now() where id=$1`, item.id, status, step); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"saga_id": item.id, "player_id": item.playerID, "status": status, "step": step})
		if _, err := tx.Exec(ctx, `
insert into outbox(event_id, aggregate_id, event_type, payload)
values($1, $2, 'saga.finished', $3)
on conflict do nothing`, item.id+":finished", item.playerID, payload); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}
