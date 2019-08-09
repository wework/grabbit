package gbus

import (
	"strings"
)

//MessageFilter matches rabbitmq topic patterns
type MessageFilter struct {
	Exchange   string
	RoutingKey string
	MsgName    string
}

//Matches the passed in exchange, routingKey, msgName with the defined filter
func (filter *MessageFilter) Matches(exchange, routingKey, msgName string) bool {

	targetExchange := strings.ToLower(exchange)
	targetRoutingKey := strings.ToLower(routingKey)
	targetMsgName := strings.ToLower(msgName)

	//if the registration is for a command then routingkeys must exactly match
	if filter.Exchange == "" && targetExchange == "" {
		return (filter.RoutingKey == targetRoutingKey) && (filter.MsgName == targetMsgName)
	}
	//if the exchanges do not matche return false
	if filter.Exchange != targetExchange {
		return false
	}

	//to enable subscribers to handle different message types published to a topic or only receive message from a specific type check if the topic matches and regMsgName is empty
	// or the topic matches and the msg types are the same
	routingKeyMatches := wildcardMatch(targetRoutingKey, filter.RoutingKey)
	return (routingKeyMatches && filter.MsgName == "") || (routingKeyMatches && filter.MsgName == targetMsgName)
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

//Registration represents a message handler's registration for a given exchange, topic and msg combination
type Registration struct {
	info    *MessageFilter
	Handler MessageHandler
}

//Matches the registration with the given xchange, routingKey, msgName
func (sub *Registration) Matches(exchange, routingKey, msgName string) bool {
	return sub.info.Matches(exchange, routingKey, msgName)
}

//NewRegistration creates a new registration
func NewRegistration(exchange, routingKey string, message Message, handler MessageHandler) *Registration {
	reg := Registration{
		info:    NewMessageFilter(exchange, routingKey, message),
		Handler: handler}
	return &reg
}

//NewMessageFilter creates a new MessageFilter
func NewMessageFilter(exchange, routingKey string, message Message) *MessageFilter {
	filter := &MessageFilter{
		Exchange:   strings.ToLower(exchange),
		RoutingKey: strings.ToLower(routingKey)}
	if message != nil {
		filter.MsgName = strings.ToLower(message.SchemaName())
	}
	return filter
}
