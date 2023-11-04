# Gocached

## Description

An attempt to build a Redis clone in Go

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

## Installation

```go
go build main.go
./build
```

## Usage

```bash

telnet localhost 6969
SET NAME GOCACHED
GET NAME
HMSET PERSON NAME RAJ SURNAME PATEL
GET PERSON

```

## License

MIT
