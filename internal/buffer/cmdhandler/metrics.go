package cmdhandler

import (
	"time"

	"github.com/ntail-io/streams/internal/buffer/commands"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	histCmdHandlerHandle = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "cmdhandler_handle"}, []string{"command"})
	obsCmdHandlerHandle  = [commands.CommandCount]prometheus.Observer{
		histCmdHandlerHandle.WithLabelValues(commands.CommandType(0).String()),
		histCmdHandlerHandle.WithLabelValues(commands.CommandType(1).String()),
		histCmdHandlerHandle.WithLabelValues(commands.CommandType(2).String()),
		histCmdHandlerHandle.WithLabelValues(commands.CommandType(3).String()),
	}
)

func measureHandle(since time.Time, cmd commands.Command) {
	obsCmdHandlerHandle[cmd.CommandType()].Observe(float64(time.Since(since).Nanoseconds()))
}
