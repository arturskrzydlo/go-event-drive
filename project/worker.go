package main

import "context"

type Task int

const (
	TaskIssueReceipt Task = iota
	TaskAppendToTracker
)

type Message struct {
	task     Task
	ticketID string
}

type Worker struct {
	queue              chan Message
	receiptsClient     ReceiptsClient
	spreadsheetsClient SpreadsheetsClient
}

func NewWorker(receiptsClient ReceiptsClient, spreadsheetsClient SpreadsheetsClient) *Worker {
	return &Worker{
		queue:              make(chan Message, 100),
		receiptsClient:     receiptsClient,
		spreadsheetsClient: spreadsheetsClient,
	}
}

func (w *Worker) Send(msgs ...Message) {
	for _, message := range msgs {
		w.queue <- message
	}
}

func (w *Worker) Run() {
	for message := range w.queue {
		var err error
		switch message.task {
		case TaskIssueReceipt:
			err = w.receiptsClient.IssueReceipt(context.Background(), message.ticketID)
		case TaskAppendToTracker:
			err = w.spreadsheetsClient.AppendRow(context.Background(), "tickets-to-print", []string{message.ticketID})
		}
		if err != nil {
			w.Send(message)
		}
	}
}
