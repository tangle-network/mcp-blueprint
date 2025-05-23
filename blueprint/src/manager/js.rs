use futures::TryFutureExt;
use rmcp::transport::TokioChildProcess;
use std::collections::BTreeMap;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::manager::McpRunner;
use crate::transport::SseServer;

/// JavaScript runner
///
/// This runner uses the `bun` package to run JavaScript scripts
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct JsRunner;

impl McpRunner for JsRunner {
    #[tracing::instrument(skip(self), fields(%package, args, port_bindings, runtime = "js"))]
    async fn start(
        &self,
        package: String,
        args: Vec<String>,
        port_bindings: Vec<(u16, Option<u16>)>,
        env_vars: BTreeMap<String, String>,
    ) -> Result<(CancellationToken, String), Error> {
        // Ensure bun is installed
        let mut checked = self.check().await;
        blueprint_sdk::debug!(?checked, "Checking if bun is installed");
        if !matches!(checked, Ok(true)) {
            // Try to install if not present or check errored
            blueprint_sdk::debug!("Installing bun");
            self.install().await?;
            checked = self.check().await;
            if !matches!(checked, Ok(true)) {
                blueprint_sdk::debug!(?checked, "bun install status");
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "bun is not installed and could not be installed",
                )));
            }
        }

        // Determine endpoint from first host port
        let endpoint = port_bindings
            .first()
            .map(|(host_port, _)| format!("127.0.0.1:{}", host_port))
            .ok_or_else(|| Error::MissingPortBinding)?;

        let factory = move || {
            let mut cmd = Command::new("bunx");
            cmd.arg("-y").arg(&package).arg("--");
            for arg in &args {
                cmd.arg(arg);
            }
            for (k, v) in env_vars.iter() {
                cmd.env(k, v);
            }

            let transport = TokioChildProcess::new(&mut cmd)?;
            Ok(transport)
        };
        let ct = SseServer::serve(endpoint.parse()?).await?.forward(factory);
        Ok((ct, endpoint))
    }

    async fn check(&self) -> Result<bool, Error> {
        let status = Command::new("bun")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(Error::Io)
            .await?;
        Ok(status.success())
    }

    #[tracing::instrument(skip(self), fields(runtime = "js"))]
    async fn install(&self) -> Result<(), Error> {
        blueprint_sdk::debug!("Installing bun");
        let output = Command::new("sh")
            .arg("-c")
            .arg("curl -fsSL https://bun.sh/install | bash")
            .status()
            .map_err(Error::Io)
            .await?;
        if output.success() {
            blueprint_sdk::debug!("bun installed successfully");
            Ok(())
        } else {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "bun installation script failed",
            )))
        }
    }
}
