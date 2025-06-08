package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type Redis struct {
	db      map[string][]string
	walFile *os.File
	lock    sync.RWMutex
	walLock sync.Mutex
}

func (c *Redis) NewRedisServer() {
	c.db = make(map[string][]string)

	var err error
	c.walFile, err = os.OpenFile("data.wal", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Could not open WAL file: %s", err)
	}

	log.Printf("WAL file opened successfully: %s", c.walFile.Name())
	c.loadSnapshot()
}
func (c *Redis) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if err := c.walFile.Close(); err != nil {
		log.Printf("Could not close WAL file: %s", err)
	}
	c.saveSnapshot()
}

func (c *Redis) set(key string, value []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.db[key] = value
}
func (c *Redis) get(key string) []string {
	c.lock.Lock()
	defer c.lock.Unlock()
	val := c.db[key]
	return val
}
func (c *Redis) del(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.db, key)
}

func (c *Redis) Operation(cmd RedisCommand, walEnabled bool) (string, error) {
	writeCommands := []string{"SET", "HMSET", "DEL"}
	if walEnabled && slices.Contains(writeCommands, cmd.command) {
		c.appendToWAL(cmd)
	}
	switch cmd.command {
	case "PING":
		return "PONG", nil
	case "GET":
		val := c.get(cmd.key)
		return strings.Join(val, " "), nil
	case "SET", "HMSET":
		c.set(cmd.key, cmd.value)
		return "OK", nil
	case "DEL":
		c.del(cmd.key)
		return "OK", nil
	}
	return "", fmt.Errorf("invalid Command")
}

func (c *Redis) appendToWAL(cmd RedisCommand) {
	c.walLock.Lock()
	defer c.walLock.Unlock()
	entry := cmd.command + " " + cmd.key + " " + strings.Join(cmd.value, " ") + "\n"
	c.walFile.WriteString(entry)
}
func (c *Redis) saveSnapshot() {
	c.lock.RLock()
	defer c.lock.RUnlock()

	tempFile, err := os.OpenFile("data.dat", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Failed to create temp snapshot file: %s", err)
		return
	}
	defer tempFile.Close()

	encoder := gob.NewEncoder(tempFile)
	if err := encoder.Encode(c.db); err != nil {
		log.Printf("Snapshot encode error: %s", err)
		return
	}

	// Replace the old file
	if err := os.Rename(tempFile.Name(), "data.dat"); err != nil {
		log.Printf("Failed to replace snapshot file: %s", err)
		return
	}

	// Clear WAL
	c.walLock.Lock()
	defer c.walLock.Unlock()
	if err := c.walFile.Truncate(0); err != nil {
		log.Printf("Could not truncate WAL file: %s", err)
	}
	if _, err := c.walFile.Seek(0, 0); err != nil {
		log.Printf("Could not seek to start of WAL file: %s", err)
	}
	if err := c.walFile.Sync(); err != nil {
		log.Printf("Could not sync WAL file: %s", err)
	}

	log.Println("Snapshot saved and WAL truncated.")
}
func (c *Redis) loadSnapshot() {
	file, err := os.Open("data.dat")
	if err != nil {
		log.Printf("Could not open snapshot file: %s", err)
		return
	}
	defer file.Close()

	c.lock.Lock()
	defer c.lock.Unlock()

	c.db = make(map[string][]string)

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&c.db); err != nil {
		log.Printf("Snapshot decode error: %s", err)
		return
	}

	log.Println("Snapshot loaded successfully.")
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		cmd := RedisCommand{}
		if err := cmd.parse(line); err == nil {
			if _, ok := c.Operation(cmd, false); ok == nil {
				log.Printf("Restored command: %s, key: %s, value: %v", cmd.command, cmd.key, cmd.value)
			} else {
				log.Printf("Error restoring command: %s", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading WAL: %s", err)
	}
}

type RedisCommand struct {
	command string
	key     string
	value   []string
}

func (cmd *RedisCommand) parse(query string) (err error) {
	query = strings.ReplaceAll(query, "\r\n", " ")
	query = strings.ReplaceAll(query, "\n", " ")
	command := strings.Fields(query)

	if len(command) < 1 {
		return fmt.Errorf("empty command")
	}

	cmd.command = strings.ToUpper(command[0])
	switch cmd.command {
	case "PING":
		{
			return nil
		}
	case "GET", "DEL":
		if len(command) < 2 {
			err := fmt.Errorf("invalid command")
			return err
		}
		cmd.key = command[1]
	case "SET":
		{
			if len(command) < 3 {
				err = fmt.Errorf("invalid command")
				return
			}
			cmd.key = command[1]
			cmd.value = []string{command[2]}
		}
	case "HMSET":
		{
			if len(command) < 2 {
				err = fmt.Errorf("invalid command")
				return
			}
			cmd.key = command[1]
			cmd.value = command[2:]
		}
	case "SUBSCRIBE":
		if len(command) < 2 {
			return fmt.Errorf("invalid SUBSCRIBE command")
		}
		cmd.key = command[1]
	case "PUBLISH":
		if len(command) < 3 {
			return fmt.Errorf("invalid PUBLISH command")
		}
		cmd.key = command[1]
		cmd.value = command[2:]

	default:
		return fmt.Errorf("unknown command: %s", cmd.command)
	}
	return nil
}

func handleConn(conn net.Conn, c *Redis) {
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Connection closed or error: %s\n", err)
			return
		}

		input := strings.TrimSpace(string(buf[:n]))
		if input == "" {
			continue
		}

		cmd := RedisCommand{}
		if err := cmd.parse(input); err != nil {
			conn.Write([]byte("Invalid command\n"))
			continue
		}

		val, err := c.Operation(cmd, true)
		if err != nil {
			conn.Write([]byte("Invalid command\n"))
		} else {
			conn.Write([]byte(val + "\n"))
		}
	}
}

func main() {
	var cache Redis
	cache.NewRedisServer()
	defer cache.Close()

	go func() {
		for {
			cache.saveSnapshot()
			// Save snapshot every 10 seconds
			// Adjust the duration as needed
			time.Sleep(10 * time.Second)
		}
	}()

	Port := 6969
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		log.Fatalf("Could not initialize the server: %s", err)
	}

	log.Printf("Listening on port %d...\n", Port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Could not accept the connection")
			continue
		}
		go handleConn(conn, &cache)
	}
}
