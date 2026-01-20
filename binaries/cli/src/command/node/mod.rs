use crate::command::Executable;

mod list;
mod start;
mod stop;

pub use list::List;
pub use start::StartNode;
pub use stop::StopNode;

/// Manage and inspect dataflow nodes.
#[derive(Debug, clap::Subcommand)]
pub enum Node {
    List(List),
    Stop(StopNode),
    Start(StartNode),
}

impl Executable for Node {
    fn execute(self) -> eyre::Result<()> {
        match self {
            Node::List(cmd) => cmd.execute(),
            Node::Stop(cmd) => cmd.execute(),
            Node::Start(cmd) => cmd.execute(),
        }
    }
}
