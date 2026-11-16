"""Test isolation: retain real gRPC instrumentation/SDK, replace network exporter only."""
import os

os.environ.setdefault("WEBHOOK_SECRET", "test-secret")
from opentelemetry.exporter.otlp.proto.grpc import trace_exporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

SPAN_EXPORTER = InMemorySpanExporter()
trace_exporter.OTLPSpanExporter = lambda **kwargs: SPAN_EXPORTER
