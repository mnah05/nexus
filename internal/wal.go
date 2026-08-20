package internal

import (
	"encoding/csv"
	"io"
	"os"
	"sync"
)

type WAL struct {
	mu      sync.Mutex
	file    *os.File
	writer  *csv.Writer
	nextIdx uint64
}

func OpenWal(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:    f,
		writer:  csv.NewWriter(f),
		nextIdx: 1,
	}, nil
}

func (w *WAL) Append(op OpType, term int, key, val string) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := w.nextIdx
	entry := WALEntry{Idx: idx, Op: op, Term: term, Key: key, Val: val}
	if err := w.writer.Write(entry.toRecord()); err != nil {
		return 0, err
	}
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return 0, err
	}

	w.nextIdx++
	return idx, nil
}

// Truncate clears the log file and resets indices. Used after a snapshot
// has captured the full state, so the log can start fresh.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := os.Truncate(w.file.Name(), 0); err != nil {
		return err
	}
	w.writer = csv.NewWriter(w.file)
	w.nextIdx = 1
	return nil
}

func (w *WAL) ReadAll() ([]WALEntry, error) {
	f, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	var entries []WALEntry

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, err
		}
		entry, err := entryFromRecord(rec)
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
