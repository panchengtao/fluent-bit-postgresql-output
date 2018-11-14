package main

import (
	"fluent-bit-postgresql-output/client"
	"github.com/fluent/fluent-bit-go/output"
	"log"
	"os"
	"strings"
)
import (
	"C"
	"fmt"
	"unsafe"
)

var pgClient *client.PgClient

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "postgresql", "PostgreSQL GO!")
}

//export FLBPluginInit
// (fluentbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctx unsafe.Pointer) int {
	// Example to retrieve an optional configuration parameter
	hosts := output.FLBPluginConfigKey(ctx, "Hosts")
	db := output.FLBPluginConfigKey(ctx, "Database")
	schema := output.FLBPluginConfigKey(ctx, "Schema")
	table := output.FLBPluginConfigKey(ctx, "Table")
	username := output.FLBPluginConfigKey(ctx, "User")
	password := output.FLBPluginConfigKey(ctx, "Password")

	config, err := client.NewPgConfig(
		hosts,
		db,
		schema,
		table,
		username,
		password)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	pgClient, err = client.NewPgClient(config)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	err = pgClient.CheckIfExist()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var ret int
	//var ts interface{}
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// Iterate Records
	var logs []string
	for {
		// Extract Record
		ret, _, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Print record keys and values
		//timestamp := ts.(output.FLBTime)
		var kvs []string
		for k, v := range record {
			kvs = append(kvs, fmt.Sprintf(" \"%s\": %v ", k, v))
		}
		var jsonb = "('{" + strings.Join(kvs, ",") + "}')"

		logs = append(logs, jsonb)

	}

	if len(logs) > 0 {
		err := pgClient.FlushLogs(logs)
		if err != nil {
			log.Println(err.Error())
		}
	} else {
		return output.FLB_RETRY
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	pgClient.Close()
	return output.FLB_OK
}

func main() {}
