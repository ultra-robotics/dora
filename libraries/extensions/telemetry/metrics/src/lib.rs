//! Enable system metric through opentelemetry exporter.
//!
//! This module fetch system information using [`sysinfo`] and
//! export those metrics via an [`opentelemetry-rust`] exporter with default configuration.
//! Observed metrics are:
//! - CPU usage.
//! - Memory and Virtual memory usage.
//! - disk usage (read and write).
//!
//! [`sysinfo`]: https://github.com/GuillaumeGomez/sysinfo
//! [`opentelemetry-rust`]: https://github.com/open-telemetry/opentelemetry-rust

use eyre::Result;
use opentelemetry::{InstrumentationScope, global, KeyValue};
use opentelemetry_otlp::MetricExporter;
use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};
use opentelemetry_system_metrics::init_process_observer;
/// Init opentelemetry meter
///
/// Use the default Opentelemetry exporter with default config
/// TODO: Make Opentelemetry configurable
///
pub fn init_metrics() -> SdkMeterProvider {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create metric exporter");

    SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .build()
}

/// Init opentelemetry meter with resource attributes
///
/// Use the default Opentelemetry exporter with default config
/// and include custom resource attributes (e.g., node status)
pub fn init_metrics_with_resource(attributes: Vec<KeyValue>) -> SdkMeterProvider {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create metric exporter");

    let resource = Resource::new(attributes);
    
    SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        .build()
}

pub async fn run_metrics_monitor(meter_id: String) -> Result<()> {
    run_metrics_monitor_with_status(meter_id, "Running".to_string()).await
}

/// Run metrics monitor with node status
///
/// The status will be included as a resource attribute on all metrics.
/// Status should be "Running" if the node is active and sending metrics,
/// or "Unknown" if the node status cannot be determined.
pub async fn run_metrics_monitor_with_status(meter_id: String, status: String) -> Result<()> {
    let attributes = vec![
        KeyValue::new("dora.node.status", status),
    ];
    let meter_provider = init_metrics_with_resource(attributes);
    global::set_meter_provider(meter_provider.clone());
    let scope = InstrumentationScope::builder(meter_id)
        .with_version("1.0")
        .build();
    let meter = global::meter_with_scope(scope);

    init_process_observer(meter).await
}
