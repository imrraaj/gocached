package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type Redis struct {
	db   map[string][]string
	lock sync.RWMutex
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
	default:
		return fmt.Errorf("unknown command: %s", cmd.command)
	}
	return nil
}

func handleConn(conn net.Conn, c *Redis) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Printf("error reading: %s\n", err)
		return
	}

	cmd := RedisCommand{}
	if err := cmd.parse(string(buffer[:n])); err != nil {
		conn.Write([]byte("Invalid command\n"))
		return
	}

	switch cmd.command {
	case "PING":
		conn.Write([]byte("PONG\n"))
	case "GET":
		val := c.get(cmd.key)
		conn.Write([]byte(strings.Join(val, " ") + "\n"))
	case "SET", "HMSET":
		c.set(cmd.key, cmd.value)
		conn.Write([]byte("OK\n"))
	case "DEL":
		c.del(cmd.key)
		conn.Write([]byte("OK\n"))
	}
}

func main() {
	var cache Redis
	cache.db = make(map[string][]string)

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
