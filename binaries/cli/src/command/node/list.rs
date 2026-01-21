use std::io::Write;

use clap::Args;
use serde::Serialize;
use tabwriter::TabWriter;
use uuid::Uuid;

use crate::{
    command::{Executable, default_tracing},
    common::CoordinatorOptions,
    formatting::OutputFormat,
};
use communication_layer_request_reply::TcpRequestReplyConnection;
use dora_message::{
    cli_to_coordinator::ControlRequest,
    coordinator_to_cli::{ControlRequestReply, NodeInfo, NodeMetricsInfo, NodeStatus},
};
use eyre::{Context, bail};

/// Maps node status and optional metrics to (status, pid, cpu, memory) for display.
fn status_columns(
    status: NodeStatus,
    metrics: Option<&NodeMetricsInfo>,
) -> (String, String, String, String) {
    match status {
        NodeStatus::Running => {
            if let Some(m) = metrics {
                (
                    "Running".to_string(),
                    m.pid.to_string(),
                    format!("{:.1}%", m.cpu_usage),
                    format!("{:.0} MB", m.memory_mb),
                )
            } else {
                ("Running".to_string(), "-".to_string(), "-".to_string(), "-".to_string())
            }
        }
        NodeStatus::Exited => (
            "Exited".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ),
        NodeStatus::Restarting => (
            "Restarting".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ),
        NodeStatus::Unknown => (
            "Unknown".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ),
    }
}

/// List all currently running nodes and their status.
///
/// Examples:
///
/// List all nodes:
///   dora node list
///
/// List nodes in a specific dataflow:
///   dora node list --dataflow my-dataflow
///
/// List nodes as JSON:
///   dora node list --format json
#[derive(Debug, Args)]
#[clap(verbatim_doc_comment)]
pub struct List {
    /// Filter by dataflow name or UUID
    #[clap(long, short = 'd', value_name = "NAME_OR_UUID")]
    pub dataflow: Option<String>,

    /// Output format
    #[clap(long, value_name = "FORMAT", default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,

    #[clap(flatten)]
    coordinator: CoordinatorOptions,
}

impl Executable for List {
    fn execute(self) -> eyre::Result<()> {
        default_tracing()?;

        let mut session = self.coordinator.connect()?;
        list(session.as_mut(), self.dataflow, self.format)
    }
}

#[derive(Serialize)]
struct OutputEntry {
    node: String,
    status: String,
    pid: String,
    cpu: String,
    memory: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataflow: Option<String>,
}

fn list(
    session: &mut TcpRequestReplyConnection,
    dataflow_filter: Option<String>,
    format: OutputFormat,
) -> eyre::Result<()> {
    // Request node information from coordinator
    let reply_raw = session
        .request(&serde_json::to_vec(&ControlRequest::GetNodeInfo).unwrap())
        .wrap_err("failed to send GetNodeInfo request")?;

    let reply: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;

    let node_infos = match reply {
        ControlRequestReply::NodeInfoList(infos) => infos,
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected reply: {other:?}"),
    };

    // Filter by dataflow if specified
    let filtered_nodes: Vec<NodeInfo> = if let Some(ref filter) = dataflow_filter {
        // Try to parse as UUID first
        let filter_uuid = Uuid::parse_str(filter).ok();

        node_infos
            .into_iter()
            .filter(|node| {
                // Match by UUID or name
                if let Some(uuid) = filter_uuid {
                    node.dataflow_id == uuid
                } else {
                    node.dataflow_name.as_deref() == Some(filter)
                }
            })
            .collect()
    } else {
        node_infos
    };

    // Convert to output entries
    let entries: Vec<OutputEntry> = filtered_nodes
        .into_iter()
        .map(|node| {
            let (status, pid, cpu, memory) = status_columns(node.status, node.metrics.as_ref());

            OutputEntry {
                node: node.node_id.to_string(),
                status,
                pid,
                cpu,
                memory,
                dataflow: if dataflow_filter.is_none() {
                    Some(
                        node.dataflow_name
                            .unwrap_or_else(|| node.dataflow_id.to_string()),
                    )
                } else {
                    None
                },
            }
        })
        .collect();

    match format {
        OutputFormat::Table => {
            let mut tw = TabWriter::new(std::io::stdout().lock());

            // Write header
            if dataflow_filter.is_none() {
                tw.write_all(b"NODE\tSTATUS\tPID\tCPU\tMEMORY\tDATAFLOW\n")?;
            } else {
                tw.write_all(b"NODE\tSTATUS\tPID\tCPU\tMEMORY\n")?;
            }

            // Write entries
            for entry in entries {
                if let Some(ref dataflow) = entry.dataflow {
                    tw.write_all(
                        format!(
                            "{}\t{}\t{}\t{}\t{}\t{}\n",
                            entry.node, entry.status, entry.pid, entry.cpu, entry.memory, dataflow
                        )
                        .as_bytes(),
                    )?;
                } else {
                    tw.write_all(
                        format!(
                            "{}\t{}\t{}\t{}\t{}\n",
                            entry.node, entry.status, entry.pid, entry.cpu, entry.memory
                        )
                        .as_bytes(),
                    )?;
                }
            }
            tw.flush()?;
        }
        OutputFormat::Json => {
            for entry in entries {
                println!("{}", serde_json::to_string(&entry)?);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_columns_running_with_metrics() {
        let m = NodeMetricsInfo {
            pid: 42,
            cpu_usage: 12.5,
            memory_mb: 100.0,
            disk_read_mb_s: None,
            disk_write_mb_s: None,
        };
        let (s, pid, cpu, mem) = status_columns(NodeStatus::Running, Some(&m));
        assert_eq!(s, "Running");
        assert_eq!(pid, "42");
        assert_eq!(cpu, "12.5%");
        assert_eq!(mem, "100 MB");
    }

    #[test]
    fn status_columns_running_without_metrics() {
        let (s, pid, cpu, mem) = status_columns(NodeStatus::Running, None);
        assert_eq!(s, "Running");
        assert_eq!(pid, "-");
        assert_eq!(cpu, "-");
        assert_eq!(mem, "-");
    }

    #[test]
    fn status_columns_exited() {
        let (s, pid, cpu, mem) = status_columns(NodeStatus::Exited, None);
        assert_eq!(s, "Exited");
        assert_eq!(pid, "-");
        assert_eq!(cpu, "-");
        assert_eq!(mem, "-");
    }

    #[test]
    fn status_columns_exited_ignores_metrics() {
        let m = NodeMetricsInfo {
            pid: 99,
            cpu_usage: 1.0,
            memory_mb: 50.0,
            disk_read_mb_s: None,
            disk_write_mb_s: None,
        };
        // Exited should show dashes even if stale metrics were present
        let (s, pid, cpu, mem) = status_columns(NodeStatus::Exited, Some(&m));
        assert_eq!(s, "Exited");
        assert_eq!(pid, "-");
        assert_eq!(cpu, "-");
        assert_eq!(mem, "-");
    }

    #[test]
    fn status_columns_restarting() {
        let (s, pid, cpu, mem) = status_columns(NodeStatus::Restarting, None);
        assert_eq!(s, "Restarting");
        assert_eq!(pid, "-");
        assert_eq!(cpu, "-");
        assert_eq!(mem, "-");
    }

    #[test]
    fn status_columns_unknown() {
        let (s, pid, cpu, mem) = status_columns(NodeStatus::Unknown, None);
        assert_eq!(s, "Unknown");
        assert_eq!(pid, "-");
        assert_eq!(cpu, "-");
        assert_eq!(mem, "-");
    }
}
