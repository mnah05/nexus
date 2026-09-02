package internal

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"sync"
)

// ErrClosed is returned when an operation is attempted on a closed WAL.
var ErrClosed = errors.New("wal: closed")

type WAL struct {
	mu      sync.Mutex
	file    *os.File
	writer  *csv.Writer
	nextIdx uint64
	closed  bool
}

func OpenWal(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:    f,
		writer:  csv.NewWriter(f),
		nextIdx: 1,
	}

	// Scan existing entries to set nextIdx properly if WAL already has data
	entries, err := w.readAllEntries()
	if err == nil && len(entries) > 0 {
		w.nextIdx = entries[len(entries)-1].Idx + 1
	}

	return w, nil
}

func (w *WAL) Append(op OpType, term int, key, val string) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
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

// Truncate clears the log file after a snapshot has captured the full state.
// The entry counter is deliberately NOT reset: WAL indices stay monotonic for
// the life of the process (and are persisted in the snapshot for restart
// continuity), so "OK <idx>" responses never loop back to small numbers.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if err := os.Truncate(w.file.Name(), 0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.writer = csv.NewWriter(w.file)
	return w.file.Sync()
}

// TruncateBefore removes all entries with Idx <= watermark, while preserving
// any entries with Idx > watermark (e.g. written while a snapshot was saving).
// Monotonic nextIdx is preserved.
func (w *WAL) TruncateBefore(watermark uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}

	w.writer.Flush()
	entries, err := w.readAllEntries()
	if err != nil {
		return err
	}

	var remaining []WALEntry
	for _, e := range entries {
		if e.Idx > watermark {
			remaining = append(remaining, e)
		}
	}

	// Truncate file to 0 and seek to start
	if err := os.Truncate(w.file.Name(), 0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	w.writer = csv.NewWriter(w.file)
	for _, e := range remaining {
		if err := w.writer.Write(e.toRecord()); err != nil {
			return err
		}
	}
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return err
	}
	return w.file.Sync()
}

// NextIdx returns the index that the next Append will assign.
func (w *WAL) NextIdx() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextIdx
}

// LastIdx returns the index of the most recently written entry (or 0 if empty).
func (w *WAL) LastIdx() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.nextIdx > 1 {
		return w.nextIdx - 1
	}
	return 0
}

// SetNextIdx resumes the counter from a recovered watermark.
func (w *WAL) SetNextIdx(n uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if n > w.nextIdx {
		w.nextIdx = n
	}
}

// Close flushes and closes the underlying log file. It is safe to call more
// than once; subsequent Appends fail with ErrClosed.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	w.writer.Flush()
	_ = w.file.Sync()
	return w.file.Close()
}

// Closed reports whether the WAL has been closed.
func (w *WAL) Closed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// ReadAll reads all entries from the log file.
func (w *WAL) ReadAll() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrClosed
	}
	w.writer.Flush()
	return w.readAllEntries()
}

func (w *WAL) readAllEntries() ([]WALEntry, error) {
	f, err := os.Open(w.file.Name())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
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
