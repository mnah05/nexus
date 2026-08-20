package internal

import (
	"fmt"
	"strconv"
)

type OpType string

const (
	OpSet OpType = "SET"
	OpDel OpType = "DEL"
)

type WALEntry struct {
	Idx  uint64
	Op   OpType
	Term int
	Key  string
	Val  string // unused/empty for OpDelete
}

func (e WALEntry) toRecord() []string {
	return []string{
		strconv.FormatUint(e.Idx, 10),
		string(e.Op),
		strconv.Itoa(e.Term),
		e.Key,
		e.Val,
	}
}
func entryFromRecord(rec []string) (WALEntry, error) {
	if len(rec) != 5 {
		return WALEntry{}, fmt.Errorf("wal: malformed record, want 5 fields got %d", len(rec))
	}
	idx, err := strconv.ParseUint(rec[0], 10, 64)
	if err != nil {
		return WALEntry{}, fmt.Errorf("wal: bad idx: %w", err)
	}
	term, err := strconv.Atoi(rec[2])
	if err != nil {
		return WALEntry{}, fmt.Errorf("wal: bad term: %w", err)
	}
	return WALEntry{
		Idx:  idx,
		Op:   OpType(rec[1]),
		Term: term,
		Key:  rec[3],
		Val:  rec[4],
	}, nil
}
