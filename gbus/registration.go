package gbus

import (
	"log"
	"strings"
)

type Registration struct {
	exchange   string
	routingKey string
	msgName    string
	Handler    MessageHandler
}

func (sub *Registration) Matches(exchange, routingKey, msgName string) bool {

	log.Printf("%v %v %v", exchange, routingKey, msgName)
	log.Printf("%v %v %v", sub.exchange, sub.routingKey, sub.msgName)
	targetExchange := strings.ToLower(exchange)
	targetRoutingKey := strings.ToLower(routingKey)
	targetMsgName := strings.ToLower(msgName)

	//if the registration is for a command then routingkeys must exactly match
	if sub.exchange == "" && targetExchange == "" {
		return (sub.routingKey == targetRoutingKey) && (sub.msgName == targetMsgName)
	}
	//if the exchanges do not matche return false
	if sub.exchange != targetExchange {
		return false
	}

	//to enable subscribers to handle different message types published to a topic or only recieve message from a specific type check if the topic matches and regMsgName is empty
	// or the topic matches and the msg types are the same
	routingKeyMatches := wildcardMatch(targetRoutingKey, sub.routingKey)
	return (routingKeyMatches && sub.msgName == "") || (routingKeyMatches && sub.msgName == targetMsgName)
}

func wildcardMatch(input, pattern string) bool {

	linput := strings.ToLower(input)
	lpattern := strings.ToLower(pattern)

	in := strings.Split(linput, ".")
	patt := strings.Split(lpattern, ".")

	return matchWords(in, patt)
}

func matchWords(input, pattern []string) bool {

	if len(input) == 0 && len(pattern) == 0 {
		return true
	}
	if len(pattern) > 1 && pattern[0] == "*" && len(input) == 0 {
		return false
	}
	if (len(pattern) >= 1 && pattern[0] == "?") || (len(pattern) != 0 && len(input) != 0 && pattern[0] == input[0]) {
		return matchWords(input[1:], pattern[1:])
	}
	if len(pattern) != 0 && pattern[0] == "*" {
		return matchWords(input, pattern[1:]) || matchWords(input[1:], pattern)
	}
	return false
}

func NewRegistration(exchange, routingKey string, message Message, handler MessageHandler) *Registration {
	reg := Registration{
		exchange:   strings.ToLower(exchange),
		routingKey: strings.ToLower(routingKey),
		Handler:    handler}

	if message != nil {
		reg.msgName = strings.ToLower(message.SchemaName())
	}
	return &reg
}
