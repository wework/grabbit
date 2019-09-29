package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"vacation_app/messages"
	"vacation_app/trace"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/wework/grabbit/gbus"
)

var runHotelsgServiceCmd = &cobra.Command{
	Use:   "hotels",
	Short: "Run the hotels service",
	Run: func(cmd *cobra.Command, args []string) {
		svcName := "hotels-service"
		closer, err := trace.CreatetJeagerTracer(svcName)

		if err != nil {
			log.Printf("Could not initialize jaeger tracer: %s", err.Error())
			return
		}

		defer closer.Close()
		gb := createBus(svcName)
		gb.HandleMessage(messages.BookHotelCmd{}, HandleBookHotelCommand)
		gb.Start()
		defer gb.Shutdown()

		fmt.Print("Hotels service running...press any key to exit...\n")
		reader := bufio.NewReader(os.Stdin)
		reader.ReadString('\n')
	},
}

func HandleBookHotelCommand(invocation gbus.Invocation, message *gbus.BusMessage) error {
	cmd := message.Payload.(*messages.BookHotelCmd)
	destination := cmd.Destination
	response := messages.BookHotelRsp{}

	if strings.ToLower(destination) == "oslo" {
		response.Success = false
		invocation.Log().Info("can't book hotels in oslo at this time")
	} else {
		response.Success = true
		invocation.Log().Infof("booking hotel to %s", cmd.Destination)
	}

	invocation.Reply(invocation.Ctx(), gbus.NewBusMessage(response))

	return nil
}
