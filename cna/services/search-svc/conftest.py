"""Keep actual SDK/instrumentation active; replace only external OTLP delivery."""
from unittest.mock import patch

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

with patch('opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter',
           return_value=InMemorySpanExporter()):
    import app  # noqa: F401: initialize the service in this interpreter only
