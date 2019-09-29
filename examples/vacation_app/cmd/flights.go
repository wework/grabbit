package cmd

import (
	"bufio"
	"fmt"
	"os"
	"vacation_app/trace"
	"vacation_app/messages"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/wework/grabbit/gbus"
)



var runFlightsgServiceCmd = &cobra.Command{
	Use:   "flights",
	Short: "Run the flights service",
	Run: func(cmd *cobra.Command, args []string) {
		svcName := "flights-service"
		closer, err := trace.CreatetJeagerTracer(svcName)

		if err != nil {
			log.Printf("Could not initialize jaeger tracer: %s", err.Error())
			return
		}

		defer closer.Close()
		gb := createBus(svcName)

		gb.HandleMessage(messages.BookFlightsCmd{}, HandleBookFlightCommand)
	gb.HandleMessage(messages.CancelFlightsCmd{}, HandleCancelFlightCommand)

		gb.Start()
		defer gb.Shutdown()

		fmt.Print("Flights service running...press any key to exit...\n")
		reader := bufio.NewReader(os.Stdin)
		reader.ReadString('\n')
	},
}

func HandleBookFlightCommand(invocation gbus.Invocation, message *gbus.BusMessage) error {
	cmd := message.Payload.(*messages.BookFlightsCmd)
	invocation.Log().Infof("booking flight to %s", cmd.Destination)
	reply := gbus.NewBusMessage(messages.BookFlightsRsp{
		Success: true,
	})
	invocation.Reply(invocation.Ctx(), reply)

	return nil
}

func HandleCancelFlightCommand(invocation gbus.Invocation, message *gbus.BusMessage) error {
	cmd := message.Payload.(*messages.CancelFlightsCmd)
	invocation.Log().Infof("canceling flight to %s", cmd.Destination)
	reply := gbus.NewBusMessage(messages.CancelFlightsRsp{
		Success: true,
	})
	invocation.Reply(invocation.Ctx(), reply)

	return nil
}
