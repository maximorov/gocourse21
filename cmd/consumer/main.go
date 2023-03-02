package main

import (
	"context"
	"encoding/json"
	"log"
	"query/consume"
	"time"
)

func main() {
	cp := consume.NewPool(
		context.Background(),
		1,
		100,
		`q1`,
		`amqp://localhost`,
		`guest`,
		`guest`,
	)

	for _, c := range cp.Consumers() {
		if err := c.InitStream(context.Background()); err != nil {
			log.Println(err)
		}

		for {
			for {
				if !c.IsDeliveryReady {
					log.Println(`Waiting...`)
					time.Sleep(consume.ReconnectDelay)
				} else {
					break
				}
			}

			select {
			case <-c.Closed():
				continue // to get new stream in select/case
			case d := <-c.GetStream():
				res := make(map[string]any)
				if err := json.Unmarshal(d.Body, &res); err != nil {
					log.Println(err)
				}
				log.Println(res)

				if err := d.Ack(false); err != nil {
					log.Println(err)
				}
			}
		}
	}
}
