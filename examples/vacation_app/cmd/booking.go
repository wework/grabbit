package cmd

import (
	"bufio"
	"fmt"
	"os"
	"vacation_app/trace"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"vacation_app/saga"
)

func init() {
	rootCmd.AddCommand(runBookingServiceeCmd)
}

var runBookingServiceeCmd = &cobra.Command{
	Use:   "booking",
	Short: "Run the booking service",
	Run: func(cmd *cobra.Command, args []string) {
		svcName := "booking-service"
		closer, err := trace.CreatetJeagerTracer(svcName)

		if err != nil {
			log.Printf("Could not initialize jaeger tracer: %s", err.Error())
			return
		}

		defer closer.Close()
		gb := createBus(svcName)

		gb.RegisterSaga(&saga.BookingSaga{})
		gb.Start()
		defer gb.Shutdown()

		fmt.Print("Booking service running...press any key to exit...\n")
		reader := bufio.NewReader(os.Stdin)
		reader.ReadString('\n')
	},
}
