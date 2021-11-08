package database

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/dgraph-io/badger/v3"
	"github.com/romshark/eventlog/client"
)

// DB is an ACID database based on the dgraph-io/badger key-value store.
type DB struct {
	db  *badger.DB
	log *log.Logger
}

// Open opens a badger database.
// If dir == "" then an in-memory database is created.
func Open(dir string, l *log.Logger) (*DB, error) {
	db, err := badger.Open(
		badger.DefaultOptions(dir).
			WithInMemory(dir == "").
			WithLoggingLevel(badger.WARNING),
	)
	if err != nil {
		return nil, err
	}
	return &DB{
		db:  db,
		log: l,
	}, nil
}

func (d *DB) Close() error {
	d.log.Printf("closing")
	return d.db.Close()
}

// TxType defines a transaction type
type TxType bool

const (
	ReadOnly  TxType = false
	ReadWrite TxType = true
)

// WithinTx creates a new database transaction and executes fn within it.
// The transaction is automatically commited if fn returns nil.
func (d *DB) WithinTx(
	tt TxType,
	fn func(*Tx) error,
) (err error) {
	t := &Tx{tx: d.db.NewTransaction(bool(tt)), log: d.log}
	defer func() {
		if err != nil {
			t.tx.Discard()
			d.log.Printf("tx %p: discarded", t)
			return
		}
		if err = t.tx.Commit(); err != nil {
			return
		}
		d.log.Printf("tx %p: commited", t)
	}()
	d.log.Printf("created tx %p", t)
	return fn(t)
}

// Tx is a database transaction.
type Tx struct {
	tx  *badger.Txn
	log *log.Logger
}

// Delete deletes an object from the database.
func (t *Tx) Delete(object string) error {
	return t.delete("o_" + object)
}

// Set updates an object entry in the database.
func (t *Tx) Set(object string, num int64) error {
	return t.set("o_"+object, fmt.Sprintf("%d", num))
}

// SetProjectionVersion changes the projection version of the database.
func (t *Tx) SetProjectionVersion(version client.Version) error {
	return t.set("version", version)
}

// GetQuantity reads the stored quantity of a particular object type.
func (t *Tx) GetQuantity(object string) (num int64, err error) {
	v, err := t.get("o_" + object)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseInt(string(v), 10, 64)
}

// GetProjectionVersion reads the projection version of the database.
func (t *Tx) GetProjectionVersion() (client.Version, error) {
	v, err := t.get("version")
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return "", nil
		}
		return "", err
	}
	return v, nil
}

// ScanObjects calls fn for each object scanned from the database.
func (t *Tx) ScanObjects(fn func(object string, quantity int64) error) error {
	return t.scanPrefix("o_", func(key, value string) error {
		q, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parsing scanned quantity: %w", err)
		}
		return fn(key[len("o_"):], q)
	})
}

func (t *Tx) get(key string) (value string, err error) {
	i, err := t.tx.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			t.log.Printf("tx %p: getting %q: not found", t, key)
		} else {
			t.log.Printf("tx %p: getting %q: %s", t, key, err)
		}
		return "", err
	}
	if err := i.Value(func(v []byte) error {
		value = string(v)
		return nil
	}); err != nil {
		t.log.Printf("tx %p: getting %q: reading value: %s", t, key, err)
		return "", err
	}
	t.log.Printf("tx %p: getting %q: %q", t, key, value)
	return value, nil
}

func (t *Tx) set(key, value string) error {
	if err := t.tx.Set([]byte(key), []byte(value)); err != nil {
		t.log.Printf("tx %p: setting %q -> %q: %s", t, key, value, err)
		return err
	}
	t.log.Printf("tx %p: set %q -> %q", t, key, value)
	return nil
}

func (t *Tx) delete(key string) error {
	if err := t.tx.Delete([]byte(key)); err != nil {
		t.log.Printf("tx %p: deleting %q: %s", t, key, err)
		return err
	}
	t.log.Printf("tx %p: deleted %q", t, key)
	return nil
}

func (t *Tx) scanPrefix(
	prefix string,
	fn func(key, value string) error,
) (err error) {
	p := []byte(prefix)
	i := t.tx.NewIterator(badger.DefaultIteratorOptions)
	defer i.Close()

	count := 0
	for i.Seek(p); i.ValidForPrefix(p); i.Next() {
		count++
		i := i.Item()
		if err := i.Value(func(v []byte) error {
			t.log.Printf(
				"tx %p: scanned %q = %q", t, string(i.Key()), string(v),
			)
			return fn(string(i.Key()), string(v))
		}); err != nil {
			t.log.Printf(
				"tx %p: reading value of %q: %s", t, string(i.Key()), err,
			)
			return err
		}
	}
	if err != nil && err != ErrAbortScan {
		return err
	}
	t.log.Printf("tx %p: scanned %d key-value pairs", t, count)
	return nil
}

var ErrAbortScan = errors.New("abort scan")
var ErrNotFound = errors.New("not found")
