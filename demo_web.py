#!/usr/bin/env python3
"""Demo web server with OpenTelemetry tracing."""

import json
import time
import random
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

# Setup OTel tracing
resource = Resource.create({"service.name": "demo-web", "service.version": "1.0.0"})
provider = TracerProvider(resource=resource)
endpoint = os.environ.get("OTEL_ENDPOINT", "http://agent:4318/v1/traces")
exporter = OTLPSpanExporter(endpoint=endpoint)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("demo-web")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        with tracer.start_as_current_span(
            "http_request", attributes={"http.method": "GET", "http.url": self.path}
        ) as span:
            # Simulate variable latency
            latency = random.uniform(0.01, 0.1)
            time.sleep(latency)
            span.set_attribute("http.latency_ms", latency * 1000)

            # Randomly return errors
            if random.random() < 0.1:
                span.set_attribute("http.status_code", 500)
                span.set_status(trace.StatusCode.ERROR, "Internal Server Error")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"Internal Server Error")
            else:
                span.set_attribute("http.status_code", 200)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok", "path": self.path}).encode())

    def log_message(self, format, *args):
        pass  # Suppress logging


if __name__ == "__main__":
    print("Starting OTel-instrumented web server on :8080")
    HTTPServer(("", 8080), Handler).serve_forever()
