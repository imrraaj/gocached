package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

type Redis struct {
	db map[string][]string
}

func (c *Redis) set(key string, value []string) {
	c.db[key] = value
}
func (c *Redis) get(key string) []string {
	val := c.db[key]
	return val
}
func (c *Redis) del(key string) {
	delete(c.db, key)
}

type RedisCommand struct {
	command string
	key     string
	value   []string
}

func (cmd *RedisCommand) parse(query string) (err error) {

	query = strings.Replace(query, "\r\n", " ", -1)
	query = strings.Replace(query, "\n", " ", -1)
	queryArray := strings.Split(query, " ")

	var command []string
	for _, v := range queryArray {
		if v != "" {
			command = append(command, v)
		}
	}
	cmd.command = command[0]
	switch cmd.command {
	case "PING":
		{
			return nil
		}
	case "GET":
		{
			if len(command) < 2 {
				err := fmt.Errorf("invalid command")
				return err
			}
			cmd.key = command[1]
		}
	case "SET":
		{
			if len(command) < 3 {
				err = fmt.Errorf("invalid command")
				return
			}
			cmd.key = command[1]
			cmd.value = append(cmd.value, command[2])
		}
	case "DEL":
		{
			if len(command) < 2 {
				err = fmt.Errorf("invalid command")
				return
			}
			cmd.key = command[1]
		}
	case "HMSET":
		{
			if len(command) < 2 {
				err = fmt.Errorf("invalid command")
				return
			}
			cmd.key = command[1]
			cmd.value = append(cmd.value, command[2:]...)
		}
	default:
		{
			err = fmt.Errorf("invalid command")
			return err
		}
	}
	return nil
}

func handleConn(conn net.Conn, c *Redis) {
	defer conn.Close()

	buffer := make([]byte, 64)
	_, err := conn.Read(buffer)

	if err != nil {
		log.Printf("error reading from connection: %s\n", err)
		return
	}

	buff := string(buffer)
	cmd := RedisCommand{}
	err = cmd.parse(buff)
	if err != nil {
		conn.Write([]byte("Invalid command\n"))
		return
	}

	switch cmd.command {
	case "PING":
		{
			conn.Write([]byte("PONG\n"))
		}
	case "GET":
		{
			val := c.get(cmd.key)
			conn.Write([]byte(strings.Join(val, " ")))
		}
	case "SET":
		{
			c.set(cmd.key, cmd.value)
			conn.Write([]byte("OK"))
		}
	case "DEL":
		{
			c.del(cmd.key)
			conn.Write([]byte("OK"))
		}
	case "HMSET":
		{
			c.set(cmd.key, cmd.value)
			conn.Write([]byte("OK"))
		}
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
