package internal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestTracing(t *testing.T) {
	// Register a test TracerProvider so that spans produce new contexts.
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	otel.SetTracerProvider(tp)

	inCtx := context.Background()

	task, ctx := StartTask(inCtx, "testing")
	assert.NotNil(t, ctx)
	assert.NotNil(t, task)
	assert.NotEqual(t, inCtx, ctx)
	task.SetTag("key", "value")

	region, ctx2 := StartRegion(ctx, "testing")
	assert.NotNil(t, ctx)
	assert.NotNil(t, region)
	assert.NotEqual(t, ctx, ctx2)
	defer task.EndTask()
	defer region.EndRegion()
}
