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
	db          map[string][]string
	walFile     *os.File
	lock        sync.RWMutex
	walLock     sync.Mutex
	subscribers map[string][]net.Conn
	subLock     sync.RWMutex
}

func (c *Redis) NewRedisServer() {
	c.db = make(map[string][]string)
	c.subscribers = make(map[string][]net.Conn)

	var err error
	c.walFile, err = os.OpenFile("data.wal", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Could not open WAL file: %s", err)
	}
	c.loadSnapshot()
}

func (c *Redis) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.walFile.Close()
	c.saveSnapshot()
}

func (c *Redis) set(key string, value []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.db[key] = value
}

func (c *Redis) get(key string) []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.db[key]
}

func (c *Redis) del(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.db, key)
}

func (c *Redis) publish(topic, message string) {
	c.subLock.RLock()
	subs := c.subscribers[topic]
	c.subLock.RUnlock()

	for _, conn := range subs {
		if _, err := conn.Write([]byte(fmt.Sprintf("[PUB][%s] %s\n", topic, message))); err != nil {
			log.Printf("Publish error: %s", err)
		}
	}
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
		return strings.Join(c.get(cmd.key), " "), nil
	case "SET", "HMSET":
		c.set(cmd.key, cmd.value)
		return "OK", nil
	case "DEL":
		c.del(cmd.key)
		return "OK", nil
	case "SUBSCRIBE":
		c.subLock.Lock()
		c.subscribers[cmd.key] = append(c.subscribers[cmd.key], cmd.conn)
		c.subLock.Unlock()
		return fmt.Sprintf("Subscribed to %s", cmd.key), nil
	case "PUBLISH":
		go c.publish(cmd.key, strings.Join(cmd.value, " "))
		return fmt.Sprintf("Message published to %s", cmd.key), nil
	default:
		return "", fmt.Errorf("invalid Command")
	}
}

func (c *Redis) appendToWAL(cmd RedisCommand) {
	c.walLock.Lock()
	defer c.walLock.Unlock()
	entry := fmt.Sprintf("%s %s %s\n", cmd.command, cmd.key, strings.Join(cmd.value, " "))
	c.walFile.WriteString(entry)
}

func (c *Redis) saveSnapshot() {
	c.lock.RLock()
	defer c.lock.RUnlock()

	tempFile, err := os.OpenFile("data.dat", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Snapshot creation error: %s", err)
		return
	}
	defer tempFile.Close()

	encoder := gob.NewEncoder(tempFile)
	if err := encoder.Encode(c.db); err != nil {
		log.Printf("Snapshot encode error: %s", err)
		return
	}

	if err := os.Rename(tempFile.Name(), "data.dat"); err != nil {
		log.Printf("Snapshot replace error: %s", err)
	}

	c.walLock.Lock()
	defer c.walLock.Unlock()
	c.walFile.Truncate(0)
	c.walFile.Seek(0, 0)
	c.walFile.Sync()
	log.Println("Snapshot saved and WAL cleared")
}

func (c *Redis) loadSnapshot() {
	file, err := os.Open("data.dat")
	if err != nil {
		log.Printf("Snapshot open error: %s", err)
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
	conn    net.Conn
}

func (cmd *RedisCommand) parse(query string) error {
	query = strings.ReplaceAll(query, "\r\n", " ")
	query = strings.ReplaceAll(query, "\n", " ")
	args := strings.Fields(query)
	if len(args) < 1 {
		return fmt.Errorf("empty command")
	}

	cmd.command = strings.ToUpper(args[0])
	switch cmd.command {
	case "PING":
		return nil
	case "GET", "DEL", "SUBSCRIBE":
		if len(args) < 2 {
			return fmt.Errorf("missing key")
		}
		cmd.key = args[1]
	case "SET":
		if len(args) < 3 {
			return fmt.Errorf("missing value")
		}
		cmd.key = args[1]
		cmd.value = []string{args[2]}
	case "HMSET":
		if len(args) < 3 {
			return fmt.Errorf("missing values")
		}
		cmd.key = args[1]
		cmd.value = args[2:]
	case "PUBLISH":
		if len(args) < 3 {
			return fmt.Errorf("missing message")
		}
		cmd.key = args[1]
		cmd.value = args[2:]
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
			log.Printf("Connection error: %s", err)
			return
		}
		input := strings.TrimSpace(string(buf[:n]))
		if input == "" {
			continue
		}

		cmd := RedisCommand{conn: conn}
		if err := cmd.parse(input); err != nil {
			conn.Write([]byte("Invalid command\n"))
			continue
		}

		resp, err := c.Operation(cmd, true)
		if err != nil {
			conn.Write([]byte("Invalid command\n"))
		} else {
			conn.Write([]byte(resp + "\n"))
		}
	}
}

func main() {
	var cache Redis
	cache.NewRedisServer()
	defer cache.Close()

	go func() {
		for {
			time.Sleep(20 * time.Second)
			cache.saveSnapshot()
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
