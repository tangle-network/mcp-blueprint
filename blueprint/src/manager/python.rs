use std::collections::BTreeMap;

use futures::TryFutureExt;
use rmcp::transport::TokioChildProcess;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::manager::McpRunner;
use crate::transport::SseServer;

/// Python runner
/// This runner uses the `uv` package to run Python scripts
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PythonRunner;

impl McpRunner for PythonRunner {
    #[tracing::instrument(skip(self), fields(%package, args, port_bindings, runtime = "python"))]
    async fn start(
        &self,
        package: String,
        args: Vec<String>,
        port_bindings: Vec<(u16, Option<u16>)>,
        env_vars: BTreeMap<String, String>,
    ) -> Result<(CancellationToken, String), Error> {
        // Ensure uv is installed
        let mut checked = self.check().await;
        blueprint_sdk::debug!(?checked, "Checking if uv is installed");
        if !matches!(checked, Ok(true)) {
            // Try to install if not present or check errored
            blueprint_sdk::debug!("Installing uv");
            self.install().await?;
            checked = self.check().await;
            if !matches!(checked, Ok(true)) {
                blueprint_sdk::debug!(?checked, "uv install status");
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "uv is not installed and could not be installed",
                )));
            }
        }

        // Determine endpoint from first host port
        let endpoint = port_bindings
            .first()
            .map(|(host_port, _)| format!("http://127.0.0.1:{}", host_port))
            .ok_or_else(|| Error::MissingPortBinding)?;

        let factory = move || {
            let mut cmd = Command::new("uvx");
            cmd.arg("run").arg(&package).arg("--");
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
        let status = tokio::process::Command::new("uv")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(Error::Io)
            .await?;
        Ok(status.success())
    }

    #[tracing::instrument(skip(self), fields(runtime = "python"))]
    async fn install(&self) -> Result<(), Error> {
        // Install uv
        blueprint_sdk::debug!("Installing uv");
        let uv_install_status = tokio::process::Command::new("sh")
            .arg("-c")
            .arg("curl -LsSf https://astral.sh/uv/install.sh | sh")
            .status()
            .map_err(Error::Io)
            .await?;
        blueprint_sdk::debug!(?uv_install_status, "uv install status");
        if !uv_install_status.success() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "uv installation script failed",
            )));
        }

        blueprint_sdk::debug!("uv installed successfully");
        // Install Python using uv
        let python_install_status = tokio::process::Command::new("uv")
            .arg("python")
            .arg("install")
            .status()
            .map_err(Error::Io)
            .await?;
        if python_install_status.success() {
            blueprint_sdk::debug!("Python installed successfully");
            Ok(())
        } else {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "uv python install command failed",
            )))
        }
    }
}
