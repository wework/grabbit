package gbus

import (
	"strings"
)

type MessageFilter struct {
	Exchange   string
	RoutingKey string
	MsgName    string
}

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

type Registration struct {
	info    *MessageFilter
	Handler MessageHandler
}

func (sub *Registration) Matches(exchange, routingKey, msgName string) bool {
	return sub.info.Matches(exchange, routingKey, msgName)
}

func NewRegistration(exchange, routingKey string, message Message, handler MessageHandler) *Registration {
	reg := Registration{
		info:    NewMessageFilter(exchange, routingKey, message),
		Handler: handler}
	return &reg
}

func NewMessageFilter(exchange, routingKey string, message Message) *MessageFilter {
	filter := &MessageFilter{
		Exchange:   strings.ToLower(exchange),
		RoutingKey: strings.ToLower(routingKey)}
	if message != nil {
		filter.MsgName = strings.ToLower(message.SchemaName())
	}
	return filter
}
