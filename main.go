package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

type KVStore struct {
	mu      sync.RWMutex
	store   map[string]string
	log     *os.File
	logPath string
}

func writeToLog(logPath string, cmd string, value string) error {
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return err
	}
	defer logFile.Close()
	content := fmt.Sprintf("%s %s", cmd, value)
	_, err = logFile.WriteString(content)
	if err != nil {
		fmt.Println("Error writing to file: ", err)
	}
	return nil
}

// This creates a new kvs store and initializes it
func NewKVStore(logPath string) (*KVStore, error) {
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	kv := &KVStore{
		store:   make(map[string]string),
		log:     logFile,
		logPath: logPath,
	}

	if err := kv.loadFromLog(logPath); err != nil {
		return nil, err
	}

	return kv, nil
}

func (kv *KVStore) loadFromLog(logPath string) error {
	logFile, err := os.OpenFile(logPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	// Now I need to read through each line of the logFile
	// While I do this I need to recommit the changes
	defer logFile.Close()

	scanner := bufio.NewScanner(logFile)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			continue
		}

		op := parts[0]
		key := parts[1]

		if op == "PUT" {
			kv.putReplay(key, parts[2])
		} else if op == "DELETE" {
			kv.deleteReplay(key)
		}

	}
	return nil
}

// Put will insert or update db
func (kv *KVStore) Put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	keyVal := fmt.Sprintf("%s %s\n", key, value)
	writeToLog(kv.logPath, "PUT", keyVal)
	kv.store[key] = value
}

func (kv *KVStore) putReplay(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store[key] = value
}

func (kv *KVStore) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, exists := kv.store[key]
	if !exists {
		return "", errors.New("key not found")
	}
	return value, nil
}

func (kv *KVStore) Delete(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exists := kv.store[key]
	if !exists {
		return errors.New("key not found")
	}
	// We must write before we actually do it
	writeToLog(kv.logPath, "DELETE", key)
	delete(kv.store, key)

	return nil
}

func (kv *KVStore) deleteReplay(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exists := kv.store[key]
	if !exists {
		return errors.New("key not found")
	}
	delete(kv.store, key)
	return nil
}

func main() {
	db, errors := NewKVStore("/Users/drewbuck/Desktop/GoProjects/BasicDB/db.txt")
	if errors != nil {
		fmt.Println("Problem with starting kv")
	}

	// db.Put("name", "Drew")
	// db.Put("role", "Engineer")

	val, _ := db.Get("name")
	fmt.Println("name", val)

	err := db.Delete("role")
	if err != nil {
		fmt.Println("DELETE error:", err)
	}

	_, err = db.Get("role")
	if err != nil {
		fmt.Println("Lookup error:", err)
	}
}
