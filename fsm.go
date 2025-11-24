package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// fsm implements raft.FSM interface
type fsm struct {
	db *badger.DB
}

type logEntry struct {
	Key   string
	Value string
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var e logEntry
	if err := json.Unmarshal(l.Data, &e); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(e.Key), []byte(e.Value))
	})

	if err != nil {
		return err
	}
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// For simplicity, we'll just return a dummy snapshot for now.
	// In a real implementation, we would iterate over the BadgerDB and stream it.
	return &fsmSnapshot{}, nil
}

// Restore restores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	// For simplicity, we'll just ignore the restore for now.
	return nil
}

type fsmSnapshot struct{}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Persist nothing for now
		return nil
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (f *fsmSnapshot) Release() {}
