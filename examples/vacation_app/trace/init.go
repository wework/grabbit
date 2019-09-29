package trace

import (
	"io"

	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

func CreatetJeagerTracer(svcName string) (io.Closer, error) {

	cfg := jaegercfg.Configuration{
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: false,
		},
	}

	// Initialize tracer with a logger and a metrics factory
	closer, err := cfg.InitGlobalTracer(svcName)

	return closer, err
}
