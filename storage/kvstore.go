package storage

import (
	"bufio"
	"errors"
	"io/ioutil"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("storage")

// KVStore is a key-value storage engine.
type KVStore struct {
	sync.RWMutex
	address  string
	memtable map[string]*KVEntry

	requestHandlers *RequestHandlers

	commitLogName, dumpFileName       string
	mapSize, boundarySize, dumpsIndex int
}

// KVEntry is a storage unit for a value.
type KVEntry struct {
	Value     string
	Timestamp int64
	Exist     int
}

// NewKVStore returns a new KVStore instance.
func NewKVStore(address string) *KVStore {
	kvs := &KVStore{
		address:      address,
		mapSize:      0,
		boundarySize: 128,
		dumpsIndex:   1,
	}
	kvs.memtable = make(map[string]*KVEntry)
	kvs.commitLogName = strings.Replace(address, ":", "_", -1) + "_commit.log"
	kvs.dumpFileName = strings.Replace(address, ":", "_", -1) + "_dump.log"

	requestHandlers := NewRequestHandler(kvs)
	kvs.requestHandlers = requestHandlers

	if _, err := os.Stat(kvs.commitLogName); os.IsNotExist(err) {
		os.Create(kvs.commitLogName)
	}

	kvs.repairDB()
	go kvs.flushToDumpFile()

	return kvs
}

// Get returns the KVEntry of the given key.
func (k *KVStore) Get(key string) (*KVEntry, error) {
	k.Lock()
	value, ok := k.memtable[key]
	k.Unlock()

	if !ok || value.Exist == 0 {
		return nil, errors.New("key not found")
	}
	return value, nil
}

// Put updates the value for the given key.
func (k *KVStore) Put(key, value string) error {
	entry := KVEntry{Value: value, Timestamp: time.Now().UnixNano(), Exist: 1}

	k.Lock()
	k.appendToCommitLog(key, &entry)
	k.memtable[key] = &entry
	k.Unlock()

	logger.Infof("Key-value pair (%s, %s) updated to memtable", key, value)

	return nil
}

// Delete removes the entry of the given key.
func (k *KVStore) Delete(key string) error {
	value, _ := k.Get(key)

	if value == nil {
		return errors.New("key not found")
	}
	value = &KVEntry{Value: "", Timestamp: time.Now().UnixNano(), Exist: 0}

	k.Lock()
	k.appendToCommitLog(key, value)
	k.memtable[key] = value
	k.Unlock()

	return nil
}

// Count returns the number of entries in local KVS.
func (k *KVStore) Count() int {
	return len(k.memtable)
}

// RegisterRPCHandlers registers the internal RPC handlers.
func (k *KVStore) RegisterRPCHandlers(server *rpc.Server) error {
	server.RegisterName("KVS", k.requestHandlers)
	logger.Info("Internal KVS request RPC handlers registered")
	return nil
}

func (k *KVStore) appendToCommitLog(key string, entry *KVEntry) error {
	fLogfile, err := os.OpenFile(k.commitLogName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logger.Error(err.Error())
		return err
	}

	k.writeKeyValueToFile(fLogfile, key, entry)
	logger.Infof("Key-value pair (%s, %s) appended to commit log", key, entry.Value)

	return nil
}

func (k *KVStore) flushToDumpFile() error {
	for {
		time.Sleep(30 * time.Second)

		k.Lock()
		f, _ := os.Create(k.dumpFileName)
		for key, value := range k.memtable {
			k.writeKeyValueToFile(f, key, value)
		}
		f, _ = os.Create(k.commitLogName)
		k.Unlock()

		logger.Notice("Memtable dumped to disk")
		logger.Info("Commit log cleared")
	}
}

func (k *KVStore) repairDB() {
	files, _ := ioutil.ReadDir("./")
	matchpattern := strings.Replace(k.address, ":", "_", -1) + "_(dump|commit).log"

	logger.Notice("Trying to repair key value storage...")

	for _, file := range files {
		if ok, _ := regexp.Match(matchpattern, []byte(file.Name())); ok {
			fLog, err := os.Open(file.Name())
			if err != nil {
				return
			}
			f := bufio.NewReader(fLog)
			for {
				key, value, timestamp, exist, err := k.getNextKeyValueFromFile(f)
				if err != nil {
					break
				}

				if cur, ok := k.memtable[key]; ok {
					if cur.Timestamp < timestamp {
						cur.Timestamp = timestamp
						cur.Exist = exist
						cur.Value = value
					}
				} else {
					tmpKVEntry := &KVEntry{Value: value, Timestamp: timestamp, Exist: exist}
					k.memtable[key] = tmpKVEntry
				}
			}
		}
	}
}

func (k *KVStore) getNextKeyValueFromFile(f *bufio.Reader) (string, string, int64, int, error) {
	var nextLenStr string
	var err error
	if nextLenStr, err = f.ReadString(' '); err != nil {
		return "", "", 0, 0, err
	}
	nextLenStr = nextLenStr[:len(nextLenStr)-1]
	nextLen, _ := strconv.Atoi(nextLenStr)
	readKey := ""
	for len(readKey) != nextLen {
		tmp := make([]byte, nextLen-len(readKey))
		f.Read(tmp)
		readKey += string(tmp[:])
	}
	nextLenStr, _ = f.ReadString(' ')
	nextLenStr, _ = f.ReadString(' ')
	nextLenStr = nextLenStr[:len(nextLenStr)-1]
	nextLen, _ = strconv.Atoi(nextLenStr)
	readValue := make([]byte, nextLen)
	if _, err = f.Read(readValue); err != nil {
		return "", "", 0, 0, err
	}

	readTimestamp, err := f.ReadString(' ')
	readTimestamp, err = f.ReadString(' ')
	readTimestamp = readTimestamp[:len(readTimestamp)-1]
	timestamp, _ := strconv.ParseInt(readTimestamp, 10, 64)

	readExist, err := f.ReadString('\n')
	readExist = readExist[:len(readExist)-1]
	exist, _ := strconv.Atoi(readExist)

	return string(readKey[:]), string(readValue[:]), timestamp, exist, nil
}

func (k *KVStore) writeKeyValueToFile(f *os.File, key string, value *KVEntry) error {
	keyString := strconv.Itoa(len(key)) + " " + key
	if _, err := f.WriteString(keyString + " "); err != nil {
		logger.Error(err.Error())
	}

	valueString := strconv.Itoa(len(value.Value)) + " " + value.Value
	if _, err := f.WriteString(valueString + " "); err != nil {
		logger.Error(err.Error())
	}

	timeStampString := strconv.FormatInt(value.Timestamp, 10)
	if _, err := f.WriteString(timeStampString + " "); err != nil {
		logger.Error(err.Error())
	}

	existString := strconv.Itoa(value.Exist)
	if _, err := f.WriteString(existString + "\n"); err != nil {
		logger.Error(err.Error())
	}

	return nil
}
