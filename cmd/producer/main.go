package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/romshark/eventlog-example/cli"
	"github.com/romshark/eventlog-example/database"
	"github.com/romshark/eventlog-example/event"

	"github.com/romshark/eventlog/client"
	"github.com/romshark/eventlog/eventlog"
)

func main() {
	var fHost string
	var fDBDir string
	var fEnableDBLog bool
	flag.StringVar(
		&fHost, "log-addr", "localhost:9090", "event log server address",
	)
	flag.StringVar(
		&fDBDir, "db-dir", "", "database directory",
	)
	flag.BoolVar(
		&fEnableDBLog, "db-log", false, "enable database debug logging",
	)
	flag.Parse()

	lApp := log.New(os.Stdout, "APP:", log.LstdFlags)
	lDB := log.New(os.Stdout, "DB:", log.LstdFlags)
	if !fEnableDBLog {
		lDB.SetOutput(io.Discard)
	}

	db, err := database.Open(fDBDir, lDB)
	if err != nil {
		lApp.Fatalf("opening database: %s", err)
	}
	defer db.Close()

	httpc := client.NewHTTP(
		fHost,
		log.New(os.Stdout, "EVENTLOG CLIENT ERR:", log.LstdFlags),
		nil, nil,
	)
	httpc.SetRetryInterval(time.Second)
	ec := client.New(httpc)

	p := &Producer{
		db:  db,
		c:   ec,
		log: lApp,
	}
	go func() {
		if err := p.Run(context.Background()); err != nil {
			if !errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				lApp.Fatalf("running producer: %s", err)
			}
		}
	}()

	fmt.Println(`commands: `)
	fmt.Println(`  put/take <num> <object>: puts or takes n objects`)
	fmt.Println(`  exit:  exits the program`)
	fmt.Println("---------------------")
	if err := cli.ScanLines(func(ln string) error {
		switch ln {
		case "exit":
			return cli.ErrAbortScan
		default:
			op, obj, quant, err := parseInput(ln)
			if err != nil {
				lApp.Printf("ERR: parsing input: %s\n", err)
				return nil
			}

			switch op {
			case "put":
				return p.Put(context.Background(), obj, quant)
			case "take":
				if err := p.Take(
					context.Background(), obj, quant,
				); err != nil {
					if errors.Is(err, ErrInsuffQuant) {
						lApp.Printf(
							"ERR: can't take %d %s, insufficient number of %s",
							quant, obj, obj,
						)
						return nil
					}
					return err
				}
			default:
				panic("unsupported operation")
			}
		}
		return nil
	}); err != nil {
		lApp.Fatalf("ERR CLI: %s", err)
	}
}

// Producer is an event producer and an aggregate enforcing invariants.
// It stores its projection of the current state of the world in a database.
type Producer struct {
	db  *database.DB
	c   *client.Client
	log *log.Logger
}

// Run synchronizes the database and begins listening for new events
// as long as ctx is not canceled.
func (p *Producer) Run(ctx context.Context) (err error) {
	if _, err := p.Sync(context.Background(), nil); err != nil {
		return fmt.Errorf("synchronizing: %w", err)
	}

	p.log.Printf("listening for updates")
	return p.c.Listen(ctx, func(v client.Version) {
		p.log.Printf("update received, log version: %s", string(v))
		if _, err = p.Sync(ctx, nil); err != nil {
			err = fmt.Errorf("synchronizing: %w", err)
			return
		}
	})
}

// Put puts objects of the given type onto the pile.
func (p *Producer) Put(
	ctx context.Context,
	object string,
	quantity int64,
) error {
	if err := ValidateInput(object, quantity); err != nil {
		return err
	}

	// Put operations don't require invariant checking
	ev, err := event.Encode(event.Event{
		Operation: "put",
		Object:    object,
		Quantity:  quantity,
	})
	if err != nil {
		return err
	}

	_, _, _, err = p.c.Append(ctx, ev)
	return err
}

// Take takes objects of the given type from the pile.
// ErrInsuffQuant is returned if there aren't enough instances stored.
func (p *Producer) Take(
	ctx context.Context,
	object string,
	quantity int64,
) error {
	if err := ValidateInput(object, quantity); err != nil {
		return err
	}
	return p.db.WithinTx(database.ReadWrite, func(t *database.Tx) error {
		// Get the current version projected by the database
		// and try to append a Take event onto it.
		v, err := t.GetProjectionVersion()
		if err != nil {
			return fmt.Errorf("reading projection version: %w", err)
		}
		_, _, _, err = p.c.TryAppend(
			ctx, v,
			// Transaction will either return ErrInsuffQuant if there aren't
			// enough instances of the requested object stored in the database
			// or the Take event that's written to the eventlog.
			func() (client.EventData, error) {
				// Make sure there's enough instances of the object stored!
				q, err := t.GetQuantity(object)
				if err != nil {
					return eventlog.EventData{}, err
				}
				if q-quantity < 0 {
					return eventlog.EventData{}, ErrInsuffQuant
				}

				ev, err := event.Encode(event.Event{
					Operation: "take",
					Object:    object,
					Quantity:  quantity,
				})
				return ev, err
			},
			// Sync will be called if client.AppendCheck fails due to a
			// client.ErrMismatchingVersions error, which indicates
			// that the projection of this service is outdated and must
			// first be updated to make sure no invariants are accepted.
			func() (client.Version, error) { return p.Sync(ctx, t) },
		)
		return err
	})
}

var ErrInsuffQuant = errors.New("insufficient quantity stored")

// Sync synchronizes the database against the eventlog applying any
// relevant event. If tx == nil then the synchronization will be executed
// within a new transaction. Sync returns the latestVersion it synchronized to.
func (p *Producer) Sync(
	ctx context.Context,
	tx *database.Tx,
) (latestVersion client.Version, err error) {
	p.log.Printf("synchronizing")
	if tx != nil {
		return p.sync(ctx, tx)
	}
	err = p.db.WithinTx(database.ReadWrite, func(tx *database.Tx) error {
		latestVersion, err = p.sync(ctx, tx)
		return err
	})
	return
}

func (p *Producer) sync(
	ctx context.Context,
	tx *database.Tx,
) (latestVersion client.Version, err error) {
	p.log.Printf("synchronizing")
	v, err := tx.GetProjectionVersion()
	if err != nil {
		return "", fmt.Errorf("reading projection version: %w", err)
	}

	sv := v
	if sv == "" {
		if sv, err = p.c.VersionInitial(ctx); err != nil {
			return "", err
		}
		p.log.Printf("starting at initial version")
	} else {
		p.log.Printf("current projection version: %s", v)
	}

	if sv == "0" {
		// Log is empty
		p.log.Printf("event log is empty")
		return sv, nil
	}

	err = p.c.Scan(ctx, sv, false, func(e client.Event) error {
		p.log.Printf(
			"scanning %s %s %s",
			e.Version, string(e.Label), string(e.PayloadJSON),
		)
		if v == e.Version {
			// Ignore the current version
			p.log.Printf("ignoring %s / %s", v, e.Version)
			return nil
		}
		if err := p.apply(tx, e); err != nil {
			return err
		}
		latestVersion = e.Version
		return nil
	})
	return
}

// apply applies e to the database within the given transaction.
func (p *Producer) apply(tx *database.Tx, e client.Event) (err error) {
	defer func() {
		if err != nil {
			return
		}
		if err = tx.SetProjectionVersion(e.Version); err != nil {
			return
		}
		p.log.Printf("update projection version: %s", e.Version)
	}()

	event, err := event.Decode(e)
	if err != nil {
		return fmt.Errorf("decoding event: %w", err)
	}

	previousQuantity, err := tx.GetQuantity(event.Object)
	if err != nil {
		return err
	}
	var newQuantity int64
	switch string(e.Label) {
	case "take":
		newQuantity = previousQuantity - int64(event.Quantity)
	case "put":
		newQuantity = previousQuantity + int64(event.Quantity)
	}

	p.log.Printf("applying version: %s", e.Version)

	if newQuantity < 1 {
		p.log.Printf("deleting object: %q", event.Object)
		return tx.Delete(event.Object)
	}

	p.log.Printf(
		"%s object %s: %d -> %d",
		e.Label, event.Object, previousQuantity, newQuantity,
	)
	return tx.Set(event.Object, newQuantity)
}

func ValidateInput(object string, quantity int64) error {
	if object == "" {
		return fmt.Errorf("invalid object: %q", object)
	}
	if quantity < 0 {
		return fmt.Errorf("invalid quantity: %d", quantity)
	}
	return nil
}

const inputRegex = `^(\w+)\s+(.+)\s+(\w+)$`

var regex = regexp.MustCompile(inputRegex)

func parseInput(in string) (op, object string, quantity int64, err error) {
	m := regex.FindAllStringSubmatch(in, -1)
	if len(m) != 1 || len(m[0]) != 4 {
		err = errors.New(
			`syntax error, input must match: ` + inputRegex,
		)
		return
	}

	switch m[0][1] {
	case "put", "take":
	default:
		err = fmt.Errorf(
			"invalid operation %q, use either \"put\" or \"take\"", m[0][1],
		)
		return
	}

	n, err := strconv.ParseInt(m[0][2], 10, 32)
	if err != nil {
		err = fmt.Errorf("parsing number: %w", err)
		return
	}

	return m[0][1], m[0][3], n, nil
}
