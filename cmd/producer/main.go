package main

import (
	"context"
	"log"
	"query/produce"
)

func main() {
	pp := produce.NewPool(
		context.Background(),
		1,
		`main`,
		`amqp://localhost`,
		`guest`,
		`guest`,
	)
	for _, p := range pp.Producers() {
		if err := p.Push(context.Background(), `key-product`, []byte(`{"someFields": "someValue 1"}`)); err != nil {
			log.Println(err)
		}
		if err := p.Push(context.Background(), `key-brand`, []byte(`{"someFields": "someValue 2"}`)); err != nil {
			log.Println(err)
		}
	}
}
