package client

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	host        = "127.0.0.1"
	port uint16 = 5432
)

type PgClient struct {
	config *PgConfig
	pool   *pgx.ConnPool
}

type PgConfig struct {
	config *pgx.ConnPoolConfig
	schema string
	table  string
}

func NewPgClient(connPoolConfig *PgConfig) (*PgClient, error) {
	pool, err := pgx.NewConnPool(*(connPoolConfig.config))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to create connection pool: %s\n", err.Error()))
	}

	return &PgClient{
		config: connPoolConfig,
		pool:   pool,
	}, nil
}

func NewPgConfig(hosts, db, schema, table, username, password string) (*PgConfig, error) {
	host, port = getHostAndPort(hosts)
	connPoolConfig := &pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     host,
			Port:     port,
			User:     username,
			Password: password,
			Database: db,
		},
		//MaxConnections: 20,
		//AcquireTimeout:time.Second,
		//AfterConnect:afterConnect
	}

	return &PgConfig{
		config: connPoolConfig,
		schema: schema,
		table:  table,
	}, nil
}

func (client *PgClient) CheckIfExist() (error) {
	schema := client.config.schema
	table := client.config.table

	tx, err := client.pool.Begin()
	if err != nil {
	}

	rows, err := tx.Query(fmt.Sprintf("select table_schema,table_name from information_schema.tables where table_name = '%s'",
		table))
	defer rows.Close()

	var tmap = make(map[string]string)
	for rows.Next() {
		var table_schema, table_name string
		rows.Scan(&table_schema, &table_name)
		tmap[table_schema] = table_name
	}

	_, ok := tmap[schema]
	if ok {
		trows, err := tx.Query(fmt.Sprintf("select column_name, data_type from information_schema.columns where table_schema = '%s' and table_name = '%s'", schema, table))
		defer trows.Close()
		if err != nil {
			return errors.New(fmt.Sprintf("Unable to check information_schema.columns info: %s\n", err.Error()))
		}

		cmap := make(map[string]string)
		for trows.Next() {
			var column_name, data_type string
			trows.Scan(&column_name, &data_type)
			cmap[column_name] = data_type
		}

		type1, ok1 := cmap["log_id"]
		type2, ok2 := cmap["log_info"]
		if ok1 && ok2 {
			if type1 != "bigint" || type2 != "jsonb" {
				return errors.New(fmt.Sprintf("Unable to create table %s.%s, database has exist similar table.", schema, table))
			}
		}
	} else {
		_, err = tx.Exec(fmt.Sprintf("create schema %s", schema))
		if err != nil {
			tx.Rollback()
			return errors.New(fmt.Sprintf("Unable to create schema: %s\n", err.Error()))
		}

		_, err = tx.Exec(fmt.Sprintf("create table %s.%s (log_id serial8 primary key, log_info jsonb)",
			schema,
			table))
		if err != nil {
			tx.Rollback()
			return errors.New(fmt.Sprintf("Unable to create table: %s\n", err.Error()))
		}
	}

	tx.Commit()
	return nil
}

func (client *PgClient) FlushLogs(logs []string) error {
	values := strings.Join(logs, ",")
	var sql = fmt.Sprintf("insert into %s.%s(log_info) values %s", client.config.schema, client.config.table, values)
	_, err := client.pool.Exec(sql)
	return err
}

func (client *PgClient) Close() {
	client.pool.Close()
}

func getHostAndPort(hosts string) (string, uint16) {
	hostp := strings.Split(hosts, ":")
	if len(hostp) > 1 {
		host = hostp[0]
		portInt, err := strconv.Atoi(hostp[1])
		if err != nil {
			log.Printf("Unable to convert port string to int: %s\n", err.Error())
			os.Exit(1)
		}

		port = uint16(portInt)
	} else if len(host) == 1 {
		host = hostp[0]
	}

	return host, port
}
