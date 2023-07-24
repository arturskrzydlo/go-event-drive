package main

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
)

type AlarmClient interface {
	StartAlarm() error
	StopAlarm() error
}

func ConsumeMessages(sub message.Subscriber, alarmClient AlarmClient) {
	messages, err := sub.Subscribe(context.Background(), "smoke_sensor")
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		smokeDetected := string(msg.Payload)
		if smokeDetected == "0" {
			if err = alarmClient.StopAlarm(); err != nil {
				fmt.Print(err.Error())
				msg.Nack()
				continue
			}
			msg.Ack()
		}
		if smokeDetected == "1" {
			if err = alarmClient.StartAlarm(); err != nil {
				fmt.Print(err.Error())
				msg.Nack()
				continue
			}
			msg.Ack()
		}
	}
}
