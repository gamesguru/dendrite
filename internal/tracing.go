// Copyright 2024 New Vector Ltd.
// Copyright 2023 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"fmt"
	"reflect"
	"runtime/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type Trace struct {
	span   oteltrace.Span
	region *trace.Region
	task   *trace.Task
}

func StartTask(inCtx context.Context, name string) (Trace, context.Context) {
	ctx, task := trace.NewTask(inCtx, name)
	ctx, span := otel.Tracer("zendrite").Start(ctx, name)
	return Trace{
		span: span,
		task: task,
	}, ctx
}

func StartRegion(inCtx context.Context, name string) (Trace, context.Context) {
	region := trace.StartRegion(inCtx, name)
	ctx, span := otel.Tracer("zendrite").Start(inCtx, name)
	return Trace{
		span:   span,
		region: region,
	}, ctx
}

func (t Trace) EndRegion() {
	t.span.End()
	if t.region != nil {
		t.region.End()
	}
}

func (t Trace) EndTask() {
	t.span.End()
	if t.task != nil {
		t.task.End()
	}
}

func (t Trace) SetTag(key string, value any) {
	var attr attribute.KeyValue
	switch v := value.(type) {
	case string:
		attr = attribute.String(key, v)
	case bool:
		attr = attribute.Bool(key, v)
	case int:
		attr = attribute.Int(key, v)
	case int64:
		attr = attribute.Int64(key, v)
	case float64:
		attr = attribute.Float64(key, v)
	default:
		// Use reflect to handle named numeric types (e.g. types.RoomNID).
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			attr = attribute.Int64(key, rv.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			attr = attribute.Int64(key, int64(rv.Uint()))
		case reflect.Float32, reflect.Float64:
			attr = attribute.Float64(key, rv.Float())
		case reflect.Bool:
			attr = attribute.Bool(key, rv.Bool())
		default:
			attr = attribute.String(key, fmt.Sprintf("%v", value))
		}
	}
	t.span.SetAttributes(attr)
}
