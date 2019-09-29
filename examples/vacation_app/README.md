## grabbit example vacation app

The following example simulates a vacation booking app and demonstrates the use of grabbit 
to manage the booking process.

The app is made up out of the following components

- The client, a console app that you enter your vacation destination and calls the booking service to 
 book the vacation.

- Booking Service manages the booking saga calling the hotels and flights services to book hotels and flights for the requested destination. it handles as well as applying flight cancelation in case the hotel service can not book a hotel.
 The booking saga sends back a message to the client with the result of the booking request once the saga completes.

- Flights Service, books flights to the requested destination and replies back the response.
 The flight service.

- Hotels Service, books hotels for the requested destination and replies back the response.
 In this example requesting to book a vacation in Oslo results in a reply message with a Failed status
 Triggering the booking saga to send a command to the flight service to cancel the booked flights.

## building and running the example

 - go build
 - docker-compose up
 - run the booking servce: vacation_app booking
 - run the hotels servce: vacation_app hotels
 - run the flights servce: vacation_app flights
 - run the client: vacation_app client

 Once the services are running you can enter a vacation destination in the client app which will invoke the booking saga which will orchestrate the flights and hotels services.


 You can see a trace of the calls by browsing to the Jeager client at http://localhost:16686
  

