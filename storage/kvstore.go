package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("storage")

type KVStore struct {
	sync.RWMutex
	address string
	db      map[string]*KVEntry

	requestHandlers *RequestHandlers

	mapSize, boundarySize, dumpsIndex int
}

type KVEntry struct {
	Value     string
	Timestamp int64
	Exist     int
}

func NewKVStore(address string) *KVStore {
	kvs := &KVStore{
		address:      address,
		mapSize:      0,
		boundarySize: 128,
		dumpsIndex:   1,
	}
	kvs.db = make(map[string]*KVEntry)
	logfileName := "commitlog.txt"
	if _, err := os.Stat(logfileName); os.IsNotExist(err) {
		os.Create(logfileName)
	}

	go kvs.dumpsDisk()
	requestHandlers := NewRequestHandler(kvs)
	kvs.requestHandlers = requestHandlers
	kvs.RepairDB()
	return kvs
}

func (k *KVStore) Get(key string) (*KVEntry, error) {
	k.Lock()
	value, ok := k.db[key]
	k.Unlock()

	if !ok || value.Exist == 0 {
		return nil, errors.New("key not found")
	}
	return value, nil
}

func (k *KVStore) Put(key, value string) error {
	tmpKVEntry := KVEntry{Value: value, Timestamp: time.Now().UnixNano(), Exist: 1}
	k.Lock()
	k.WriteLog(key, &tmpKVEntry)
	k.db[key] = &tmpKVEntry
	k.Unlock()

	logger.Infof("Key-value pair (%s, %s) updated", key, value)

	return nil
}

func (k *KVStore) Delete(key string) error {

	tmp, _ := k.Get(key)

	if tmp == nil {
		return errors.New("key not found")
	}
	tmp = &KVEntry{Value: "", Timestamp: time.Now().UnixNano(), Exist: 0}
	k.Lock()
	k.WriteLog(key, tmp)
	k.db[key] = tmp
	k.Unlock()
	return nil
}

func (k *KVStore) RegisterRPCHandlers(server *rpc.Server) error {
	server.RegisterName("KVS", k.requestHandlers)
	logger.Info("Internal KVS request RPC handlers registered")
	return nil
}

func (k *KVStore) WriteLog(key string, value *KVEntry) error {
	logfileName := "commitlog.txt"
	fLogfile, ok := os.OpenFile(logfileName, os.O_APPEND, 0644)
	if ok != nil {
		fmt.Println(ok)
	}
	k.writeKeyValueToFile(fLogfile, key, value)
	return nil
}

func (k *KVStore) dumpsDisk() error {
	for {
		time.Sleep(30 * time.Second)
		k.Lock()
		dumpsFileName := "dump.txt"
		f, _ := os.Create(dumpsFileName)
		for key, value := range k.db {
			k.writeKeyValueToFile(f, key, value)
		}
		logfileName := "commitlog.txt"
		f, _ = os.Create(logfileName)
		k.Unlock()
	}
	return nil
}

func (k *KVStore) RepairDB() {
	files, _ := ioutil.ReadDir("./")
	matchpattern := "(dump|commitlog).txt"
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

				if cur, ok := k.db[key]; ok {
					if cur.Timestamp < timestamp {
						cur.Timestamp = timestamp
						cur.Exist = exist
						cur.Value = value
					}
				} else {
					tmpKVEntry := &KVEntry{Value: value, Timestamp: timestamp, Exist: exist}
					k.db[key] = tmpKVEntry
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
	if _, ok := f.WriteString(keyString + " "); ok != nil {
		fmt.Println("Wrinting Log Error")
	}
	valueString := strconv.Itoa(len(value.Value)) + " " + value.Value
	if _, ok := f.WriteString(valueString + " "); ok != nil {
		fmt.Println("Wrinting Log Error")
	}
	timeStampString := strconv.FormatInt(value.Timestamp, 10)
	if _, ok := f.WriteString(timeStampString + " "); ok != nil {
		fmt.Println("Wrinting Log Error")
	}
	existString := strconv.Itoa(value.Exist)
	if _, ok := f.WriteString(existString + "\n"); ok != nil {
		fmt.Println("Wrinting Log Error")
	}
	return nil
}

func (k *KVStore) Count() int {
	return len(k.db)
}
