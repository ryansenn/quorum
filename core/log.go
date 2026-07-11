package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
)

type Command struct {
	Op    string
	Key   string
	Value string
	// Puts holds the payload for Op == "batch". Single-put commands leave it nil.
	Puts []KV `json:",omitempty"`
}

type LogEntry struct {
	Term       int64
	Command    Command
	Serialized []byte
}

type MetaData struct {
	Term     int64
	VotedFor string
}

// Snapshot is a point-in-time capture of the state machine plus the Raft
// coordinates of the last log entry it includes. Everything at or below
// LastIncludedIndex can be dropped from the log once this is persisted.
type Snapshot struct {
	LastIncludedIndex int64
	LastIncludedTerm  int64
	Data              map[string]string
}

func NewCommand(op string, key string, value string) *Command {
	return &Command{Op: op, Key: key, Value: value}
}

// NewBatchCommand packs many puts into one Raft log entry.
func NewBatchCommand(puts []KV) *Command {
	return &Command{Op: "batch", Puts: puts}
}

func NewLogEntry(term int64, command *Command) *LogEntry {
	serialized, err := json.Marshal(command)
	if err != nil {
		log.Fatal(err)
	}
	return &LogEntry{Term: term, Command: *command, Serialized: serialized}
}

type Logger struct {
	Id       string
	logFile  *os.File
	metaFile *os.File
	offset   []int64
	metaMu   sync.Mutex
	dirty    bool
	syncMu   sync.Mutex
}

func newLogger(id string) *Logger {
	os.MkdirAll("logs", 0755)
	path := "logs/" + id + ".rlog"
	logs, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatalf("%s open log: %v", id, err)
	}

	path = "logs/" + id + ".meta"
	meta, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}

	return &Logger{Id: id, logFile: logs, metaFile: meta}
}

func (l *Logger) ClearData() {
	err := l.logFile.Truncate(0)
	if err != nil {
		log.Fatal(err)
	}
	l.logFile.Seek(0, io.SeekStart)
	l.offset = nil

	// Reset must also drop any snapshot, otherwise a fresh node inherits a stale
	// snapshot base and its new entries are mapped to the wrong absolute indices.
	os.Remove("logs/" + l.Id + ".snap")
	os.Remove("logs/" + l.Id + ".snap.tmp")
	os.Remove("logs/" + l.Id + ".rlog.tmp")

	l.metaMu.Lock()
	defer l.metaMu.Unlock()

	err = l.metaFile.Truncate(0)
	if err != nil {
		log.Fatalf("%s %s", l.Id, err)
	}
	l.metaFile.Seek(0, io.SeekStart)
}

func (l *Logger) WriteMeta(term int64, votedFor string) {
	l.metaMu.Lock()
	defer l.metaMu.Unlock()

	l.metaFile.Seek(0, io.SeekStart)

	var meta MetaData
	if err := json.NewDecoder(l.metaFile).Decode(&meta); err != nil && err != io.EOF {
		log.Fatalf("%s read meta: %v", l.Id, err)
	}

	meta.Term = term
	meta.VotedFor = votedFor

	l.metaFile.Truncate(0)
	l.metaFile.Seek(0, io.SeekStart)
	json.NewEncoder(l.metaFile).Encode(meta)
}

func (l *Logger) AppendLog(entry *LogEntry) {
	data := encodeLogEntry(entry)

	pos, err := l.logFile.Seek(0, io.SeekEnd)

	if err != nil {
		log.Fatalf("%s %s", l.Id, err)
	}

	if _, err := l.logFile.Write(data); err != nil {
		log.Fatalf("%s %s", l.Id, err)
	}

	l.offset = append(l.offset, pos)
	l.dirty = true
}

func (l *Logger) Sync() {
	l.syncMu.Lock()
	defer l.syncMu.Unlock()
	if !l.dirty {
		return
	}
	if err := l.logFile.Sync(); err != nil {
		log.Fatalf("%s sync log: %v", l.Id, err)
	}
	l.dirty = false
}

// WriteSnapshot persists a snapshot atomically (write temp, fsync, rename) so a
// crash never leaves a half-written snapshot file.
func (l *Logger) WriteSnapshot(snap *Snapshot) {
	tmp := l.writeSnapshotTemp(snap)
	if err := os.Rename(tmp, "logs/"+l.Id+".snap"); err != nil {
		log.Fatalf("%s rename snapshot: %v", l.Id, err)
	}
}

// writeSnapshotTemp writes and fsyncs the snapshot to a .snap.tmp file without
// renaming it. Used by off-lock compaction so the live .snap is only replaced
// at cutover, after the rewritten log is ready.
func (l *Logger) writeSnapshotTemp(snap *Snapshot) string {
	data, err := json.Marshal(snap)
	if err != nil {
		log.Fatalf("%s marshal snapshot: %v", l.Id, err)
	}
	tmp := "logs/" + l.Id + ".snap.tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Fatalf("%s create snapshot: %v", l.Id, err)
	}
	if _, err := f.Write(data); err != nil {
		log.Fatalf("%s write snapshot: %v", l.Id, err)
	}
	if err := f.Sync(); err != nil {
		log.Fatalf("%s sync snapshot: %v", l.Id, err)
	}
	f.Close()
	return tmp
}

// writeLogTemp encodes entries to a temporary log file and fsyncs it. Callers
// later swap it into place under LogMu via swapLogFile.
func (l *Logger) writeLogTemp(entries []*LogEntry) string {
	tmp := "logs/" + l.Id + ".rlog.tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Fatalf("%s create log temp: %v", l.Id, err)
	}
	for _, entry := range entries {
		if _, err := f.Write(encodeLogEntry(entry)); err != nil {
			log.Fatalf("%s write log temp: %v", l.Id, err)
		}
	}
	if err := f.Sync(); err != nil {
		log.Fatalf("%s sync log temp: %v", l.Id, err)
	}
	f.Close()
	return tmp
}

// appendLogTemp appends entries to an existing temp log and fsyncs. Used at
// cutover for entries that arrived while the temp file was being built.
func (l *Logger) appendLogTemp(path string, entries []*LogEntry) {
	if len(entries) == 0 {
		return
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("%s open log temp: %v", l.Id, err)
	}
	for _, entry := range entries {
		if _, err := f.Write(encodeLogEntry(entry)); err != nil {
			log.Fatalf("%s append log temp: %v", l.Id, err)
		}
	}
	if err := f.Sync(); err != nil {
		log.Fatalf("%s sync log temp: %v", l.Id, err)
	}
	f.Close()
}

// swapLogFile replaces the live .rlog with a pre-synced temp file and rebuilds
// the offset table for entries. Caller holds LogMu. syncMu serializes against Sync.
func (l *Logger) swapLogFile(tmpPath string, entries []*LogEntry) {
	l.syncMu.Lock()
	defer l.syncMu.Unlock()

	if err := l.logFile.Close(); err != nil {
		log.Fatalf("%s close log: %v", l.Id, err)
	}
	final := "logs/" + l.Id + ".rlog"
	if err := os.Rename(tmpPath, final); err != nil {
		log.Fatalf("%s rename log: %v", l.Id, err)
	}
	f, err := os.OpenFile(final, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("%s reopen log: %v", l.Id, err)
	}
	l.logFile = f
	l.offset = nil
	var pos int64
	for _, entry := range entries {
		l.offset = append(l.offset, pos)
		pos += int64(len(encodeLogEntry(entry)))
	}
	l.dirty = false
}

// commitSnapshotTemp renames a previously written .snap.tmp into place.
func (l *Logger) commitSnapshotTemp(tmpPath string) {
	if err := os.Rename(tmpPath, "logs/"+l.Id+".snap"); err != nil {
		log.Fatalf("%s rename snapshot: %v", l.Id, err)
	}
}

// LoadSnapshot returns the persisted snapshot, or nil if none exists.
func (l *Logger) LoadSnapshot() *Snapshot {
	data, err := os.ReadFile("logs/" + l.Id + ".snap")
	if err != nil {
		return nil
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		log.Printf("%s load snapshot: %v", l.Id, err)
		return nil
	}
	return &snap
}

// Rewrite replaces the entire on-disk log with the given entries (the tail kept
// after compaction, or the empty tail after installing a snapshot) and rebuilds
// the offset table. Callers hold the node's LogMu so no append races this.
func (l *Logger) Rewrite(entries []*LogEntry) {
	l.syncMu.Lock()
	defer l.syncMu.Unlock()

	if err := l.logFile.Truncate(0); err != nil {
		log.Fatalf("%s truncate log: %v", l.Id, err)
	}
	if _, err := l.logFile.Seek(0, io.SeekStart); err != nil {
		log.Fatalf("%s seek log: %v", l.Id, err)
	}

	l.offset = nil
	for _, entry := range entries {
		pos, err := l.logFile.Seek(0, io.SeekEnd)
		if err != nil {
			log.Fatalf("%s %s", l.Id, err)
		}
		if _, err := l.logFile.Write(encodeLogEntry(entry)); err != nil {
			log.Fatalf("%s %s", l.Id, err)
		}
		l.offset = append(l.offset, pos)
	}
	if err := l.logFile.Sync(); err != nil {
		log.Fatalf("%s sync log: %v", l.Id, err)
	}
	l.dirty = false
}

func (l *Logger) AppendLogs(entries []*LogEntry, start int64) {
	if start < int64(len(l.offset)) {
		err := l.logFile.Truncate(l.offset[start])
		if err != nil {
			log.Fatalf("%s %s", l.Id, err)
		}
	}

	for _, entry := range entries {
		l.AppendLog(entry)
	}
	l.Sync()
}

func encodeLogEntry(entry *LogEntry) []byte {
	data, err := json.Marshal(entry)

	if err != nil {
		log.Fatal(err)
	}

	data = append(data, '\n')

	var buf bytes.Buffer
	size := uint32(len(data))
	binary.Write(&buf, binary.LittleEndian, size)
	buf.Write(data)
	final := buf.Bytes()

	return final
}

func decodeLogEntry(buf []byte) *LogEntry {
	buf = buf[:len(buf)-1]
	var entry LogEntry
	err := json.Unmarshal(buf, &entry)
	if err != nil {
		log.Fatal(err)
	}
	entry.Serialized, err = json.Marshal(&entry.Command)
	if err != nil {
		log.Fatal(err)
	}

	return &entry
}

func (l *Logger) LoadMeta() (int64, string) {
	l.metaMu.Lock()
	defer l.metaMu.Unlock()

	l.metaFile.Seek(0, io.SeekStart)

	var metaData MetaData
	decoder := json.NewDecoder(l.metaFile)
	err := decoder.Decode(&metaData)

	if err != nil {
		if err != io.EOF {
			log.Fatalf("%s %s", l.Id, err)
		}
		metaData = MetaData{}
	}

	return metaData.Term, metaData.VotedFor
}

func (l *Logger) BuildOffsetTable() {
	l.offset = nil
	l.logFile.Seek(0, io.SeekStart)

	offset := int64(0)
	var buf [4]byte
	n, err := l.logFile.Read(buf[:])

	if n != 4 {
		return
	}

	for err == nil {
		l.offset = append(l.offset, offset)
		size := int64(binary.LittleEndian.Uint32(buf[:]))
		offset, err = l.logFile.Seek(size, io.SeekCurrent)

		if err != nil {
			log.Fatalf("%s %s", l.Id, err)
		}

		n, err = l.logFile.Read(buf[:])

		if n != 4 {
			return
		}
	}

	l.offset = append(l.offset, offset)
}

func (l *Logger) LoadLogs() []*LogEntry {
	l.BuildOffsetTable()

	var entries []*LogEntry

	for _, offset := range l.offset {
		l.logFile.Seek(offset, io.SeekStart)

		var buf [4]byte
		_, err := l.logFile.Read(buf[:])
		if err != nil {
			log.Fatalf("%s %s", l.Id, err)
		}

		size := int(binary.LittleEndian.Uint32(buf[:]))
		logBuf := make([]byte, size)
		_, err = l.logFile.Read(logBuf)
		if err != nil {
			log.Fatalf("%s %s", l.Id, err)
		}

		entry := decodeLogEntry(logBuf)
		entries = append(entries, entry)
	}

	return entries
}
