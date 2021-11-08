package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/romshark/eventlog-example/cli"
	"github.com/romshark/eventlog-example/database"
	"github.com/romshark/eventlog-example/event"

	"github.com/romshark/eventlog/client"
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
		log.New(os.Stderr, "EVENTLOG CLIENT ERR:", log.LstdFlags),
		nil, nil,
	)
	httpc.SetRetryInterval(time.Second)
	ec := client.New(httpc)

	c := &Consumer{
		db:  db,
		c:   ec,
		log: lApp,
	}
	go func() {
		if err := c.Run(context.Background()); err != nil {
			if !errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				lApp.Fatalf("running consumer: %s", err)
			}
		}
	}()

	fmt.Println(`commands: `)
	fmt.Println(`  print: prints the current state of the world`)
	fmt.Println(`  exit:  exits the program`)
	fmt.Println("---------------------")
	if err := cli.ScanLines(func(ln string) error {
		switch ln {
		case "exit":
			return cli.ErrAbortScan
		case "print":
			return c.ScanDB(func(v client.Version) (resume bool) {
				if v == "" {
					c.log.Printf("projection version: log empty")
				} else {
					c.log.Printf("projection version: %s", v)
				}
				return true
			}, func(object string, num int64) (resume bool) {
				fmt.Printf(" %s: %d\n", object, num)
				return true
			})
		default:
			fmt.Printf("  unknown command: %q\n", ln)
		}
		return nil
	}); err != nil {
		c.log.Printf("ERR CLI: %s", err)
	}
}

// Consumer is an event log consumer and an aggregate.
// It stores its projection of the current state of the world in a database.
type Consumer struct {
	db  *database.DB
	c   *client.Client
	log *log.Logger
}

// Run synchronizes the database and begins listening for new events
// as long as ctx is not canceled.
func (c *Consumer) Run(ctx context.Context) (err error) {
	if err := c.Sync(context.Background()); err != nil {
		return fmt.Errorf("synchronizing: %w", err)
	}

	c.log.Printf("listening for updates")
	return c.c.Listen(ctx, func(v client.Version) {
		c.log.Printf("update received, log version: %s", string(v))
		if err = c.Sync(ctx); err != nil {
			err = fmt.Errorf("synchronizing: %w", err)
			return
		}
	})
}

// Sync synchronizes the database against the eventlog applying any
// relevant event.
func (c *Consumer) Sync(ctx context.Context) error {
	c.log.Printf("synchronizing")

	return c.db.WithinTx(database.ReadWrite, func(tx *database.Tx) error {
		v, err := tx.GetProjectionVersion()
		if err != nil {
			return fmt.Errorf("reading projection version: %w", err)
		}

		sv := v
		if sv == "" {
			if sv, err = c.c.VersionInitial(ctx); err != nil {
				return err
			}
			c.log.Printf("starting at initial version")
		} else {
			c.log.Printf("current projection version: %s", v)
		}

		if sv == "0" {
			// Log is empty
			c.log.Printf("event log is empty")
			return nil
		}

		return c.c.Scan(ctx, sv, false, func(e client.Event) error {
			c.log.Printf(
				"scanning (version: %s; label: %q; payload: %s)",
				e.Version, string(e.Label), string(e.PayloadJSON),
			)
			if v == e.Version {
				// Ignore the current version
				c.log.Printf("ignoring %s / %s", v, e.Version)
				return nil
			}
			return c.apply(tx, e)
		})
	})
}

// ScanDB calls onVersion supplying the current version
// projected by the database and proceeds to calling onObject
// for each object scanned from the database.
// ScanDB returns nil immediately if either onVersion or onObject return false.
func (c *Consumer) ScanDB(
	onVersion func(client.Version) (resume bool),
	onObject func(object string, quantity int64) (resume bool),
) error {
	return c.db.WithinTx(database.ReadOnly, func(tx *database.Tx) error {
		v, err := tx.GetProjectionVersion()
		if err != nil {
			return err
		}
		if !onVersion(v) {
			return nil
		}
		return tx.ScanObjects(func(object string, quantity int64) error {
			if !onObject(object, quantity) {
				return database.ErrAbortScan
			}
			return nil
		})
	})
}

// apply applies e to the database within the given transaction.
func (c *Consumer) apply(tx *database.Tx, e client.Event) (err error) {
	defer func() {
		if err != nil {
			return
		}
		if err = tx.SetProjectionVersion(e.Version); err != nil {
			return
		}
		c.log.Printf("update projection version: %s", e.Version)
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

	c.log.Printf("applying version: %s", e.Version)

	if newQuantity < 1 {
		c.log.Printf("deleting object: %q", event.Object)
		return tx.Delete(event.Object)
	}

	c.log.Printf(
		"%s object %s: %d -> %d",
		e.Label, event.Object, previousQuantity, newQuantity,
	)
	return tx.Set(event.Object, newQuantity)
}
