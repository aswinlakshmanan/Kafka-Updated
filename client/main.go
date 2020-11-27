package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"proto"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/grpc"
	"database/sql"
	_"github.com/go-sql-driver/mysql"
	
)

const (
	address     = "localhost:50051"
	defaultName = "world"
	username = "root"
	password = "Password"
	dbname = "testkafka"

)

func main() {

	db, err := dbConnection()
	if err != nil {
		log.Printf("db connection failed %s", err)
		return
	}
	defer db.Close()

	log.Printf("db connection successful")


	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := proto.NewClient(conn)

	username := defaultName
	name := defaultName

	if len(os.Args) > 1 {
		name = os.Args[1]
		username = os.Args[2]
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	d, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,

		"group.id":           group,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", d)

	err = d.SubscribeTopics(topics, nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:

				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()

func details(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s",username,password,hostname,dbname)
}

func dbopenconn() (*sql.DB, error) {
	db, err := sql.Open("mysql", details(""))
	if err != nil {
		log.Printf("Error encountered %s",err)
		return nil, err
	}
	defer db.Close()

	func insertservice(db *sql.DB, username string, name string) error{
		s, err := db.Prepare("INSERT INTO sample (username,name) values (?,?);")
		if err != nil {
			fmt.Print(err.Error())
		}
		_,err = stmt.Exec(username, name)

		if err != nil {
			fmt.Print(err.Error())
		}
		return nil
	}
}

}
