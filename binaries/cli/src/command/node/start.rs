use clap::Args;
use uuid::Uuid;

use crate::command::{default_tracing, Executable};
use crate::common::CoordinatorOptions;
use communication_layer_request_reply::TcpRequestReplyConnection;
use dora_message::cli_to_coordinator::ControlRequest;
use dora_message::coordinator_to_cli::ControlRequestReply;
use eyre::{bail, Context};

#[derive(Debug, Args)]
pub struct StartNode {
    /// UUID of the dataflow containing the node
    #[clap(long, short = 'd')]
    pub dataflow: Uuid,

    /// ID of the node to start
    #[clap(long, short = 'n')]
    pub node_id: String,

    #[clap(flatten)]
    coordinator: CoordinatorOptions,
}

impl Executable for StartNode {
    fn execute(self) -> eyre::Result<()> {
        default_tracing()?;
        let mut session = self.coordinator.connect()?;

        let reply_raw = session
            .request(
                &serde_json::to_vec(&ControlRequest::StartNode {
                    dataflow_uuid: self.dataflow,
                    node_id: self.node_id.into(),
                })
                .unwrap(),
            )
            .wrap_err("failed to send start node request")?;

        let reply: ControlRequestReply =
            serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;

        match reply {
            ControlRequestReply::NodeStarted {
                dataflow_uuid,
                node_id,
            } => {
                println!("Node {node_id} started in dataflow {dataflow_uuid}");
                Ok(())
            }
            ControlRequestReply::NodeStartError { error } => bail!("Failed to start node: {error}"),
            other => bail!("unexpected reply: {other:?}"),
        }
    }
}
