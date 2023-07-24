package main

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
	"net/http"
	"os"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
}

func main() {
	log.Init(logrus.InfoLevel)

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	logger := watermill.NewStdLogger(false, false)

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	redisPublisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{Client: rdb}, logger)

	receiptsClient := NewReceiptsClient(clients)
	spreadsheetsClient := NewSpreadsheetsClient(clients)

	e := commonHTTP.NewEcho()
	//go worker.Run()

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			err = redisPublisher.Publish("issue-receipt", &message.Message{UUID: watermill.NewUUID(), Payload: []byte(ticket)})
			if err != nil {
				logger.Error("error publishing issue receipt", err, watermill.LogFields{})
			}
			err = redisPublisher.Publish("append-to-tracker", &message.Message{UUID: watermill.NewUUID(), Payload: []byte(ticket)})
			if err != nil {
				logger.Error("error publishing append to tracker", err, watermill.LogFields{})
			}
		}

		return c.NoContent(http.StatusOK)
	})

	redisSubscriberReceipts, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{Client: rdb, ConsumerGroup: "issue-receipt"}, logger)
	redisSubscriberSpreadsheets, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{Client: rdb, ConsumerGroup: "spreadsheets"}, logger)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddNoPublisherHandler("receipts-handler", "issue-receipt", redisSubscriberReceipts, func(msg *message.Message) error {
		ticketID := string(msg.Payload)
		return receiptsClient.IssueReceipt(context.Background(), ticketID)
	})

	router.AddNoPublisherHandler("spreadsheets-handler", "issue-receipt", redisSubscriberSpreadsheets, func(msg *message.Message) error {
		ticketID := string(msg.Payload)
		return spreadsheetsClient.AppendRow(context.Background(), "tickets-to-print", []string{ticketID})
	})

	logrus.Info("Server starting...")

	go func() {
		err = router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	err = e.Start(":8080")
	if err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}

type ReceiptsClient struct {
	clients *clients.Clients
}

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{
		clients: clients,
	}
}

func (c ReceiptsClient) IssueReceipt(ctx context.Context, ticketID string) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: ticketID,
	}

	receiptsResp, err := c.clients.Receipts.PutReceiptsWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if receiptsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", receiptsResp.StatusCode())
	}

	return nil
}

type SpreadsheetsClient struct {
	clients *clients.Clients
}

func NewSpreadsheetsClient(clients *clients.Clients) SpreadsheetsClient {
	return SpreadsheetsClient{
		clients: clients,
	}
}

func (c SpreadsheetsClient) AppendRow(ctx context.Context, spreadsheetName string, row []string) error {
	request := spreadsheets.PostSheetsSheetRowsJSONRequestBody{
		Columns: row,
	}

	sheetsResp, err := c.clients.Spreadsheets.PostSheetsSheetRowsWithResponse(ctx, spreadsheetName, request)
	if err != nil {
		return err
	}
	if sheetsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", sheetsResp.StatusCode())
	}

	return nil
}
