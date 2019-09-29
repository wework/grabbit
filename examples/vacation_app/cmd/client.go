package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"vacation_app/messages"
	"vacation_app/trace"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/wework/grabbit/gbus"
)


var runClientCmd = &cobra.Command{
	Use:   "client",
	Short: "Run the client app",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("\033]0;Title goes here\007")
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
		log.SetLevel(log.ErrorLevel)

		svcName := "client"
		closer, err := trace.CreatetJeagerTracer(svcName)

		if err != nil {
			log.Printf("Could not initialize jaeger tracer: %s", err.Error())
			return
		}
		defer closer.Close()
		gb := createBus(svcName)

		gb.HandleMessage(messages.BookingComplete{}, HandleBookingComplete)
		gb.Start()
		defer gb.Shutdown()

		for {
			fmt.Print("Enter destination ...\n")
			reader := bufio.NewReader(os.Stdin)
			dest, _ := reader.ReadString('\n')
			dest = strings.TrimSpace(dest)
			bookVacationCmd := gbus.NewBusMessage(messages.BookVacationCmd{
				Destination: dest,
			})

			gb.Send(context.Background(), "booking-service", bookVacationCmd)

			fmt.Printf("booking vacation for destination %s\n", dest)

		}
	},
}

func HandleBookingComplete(invocation gbus.Invocation, message *gbus.BusMessage) error {
	bookingComplete := message.Payload.(*messages.BookingComplete)
	if bookingComplete.Success {
		fmt.Printf("booking completed succesfully\n")

	} else {
		fmt.Printf("failed to book vacation\n")
	}
	return nil
}
