use std::{collections::HashMap, path::PathBuf, time::Duration};

use chrono::DateTime;
use event_loop::{try_get, Handled, Is, Message, MessageHandler};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use steamid_ng::SteamID;
use tokio::sync::mpsc::{Receiver, UnboundedSender};

use crate::{
    player_records::Verdict,
    settings::FriendsAPIUsage,
    state::MACState,
    web::{MAC_VERSION, UPDATE_REPO},
};

#[derive(Debug, Clone, Copy)]
pub struct Refresh;
impl Message<MACState> for Refresh {
    fn update_state(self, state: &mut MACState) {
        state.players.refresh();
    }

    #[allow(unused_variables)]
    fn preprocess(&mut self, state: &MACState) {}
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserUpdate {
    #[serde(rename = "localVerdict")]
    pub local_verdict: Option<Verdict>,
    #[serde(rename = "customData")]
    pub custom_data: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct UserUpdates(pub HashMap<SteamID, UserUpdate>);
impl Message<MACState> for UserUpdates {
    fn update_state(self, state: &mut MACState) {
        for (k, v) in self.0 {
            let name = state.players.get_name(k).map(ToOwned::to_owned);

            // Insert record if it didn't exist
            let record = state.players.records.entry(k).or_default();

            if let Some(custom_data) = v.custom_data {
                record.set_custom_data(custom_data);
            }

            if let Some(verdict) = v.local_verdict {
                record.set_verdict(verdict);
                if let Some(name) = name {
                    record.add_previous_name(&name);
                }
            }

            if record.is_empty() {
                state.players.records.remove(&k);
            }
        }

        state.players.records.save_ok();
    }
}

#[allow(clippy::unused_async)]
pub async fn emit_on_timer<M: 'static + Send>(
    interval: Duration,
    emit: fn() -> M,
) -> Box<Receiver<M>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    let mut interval = tokio::time::interval(interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tokio::task::spawn(async move {
        loop {
            interval.tick().await;
            if matches!(tx.send(emit()).await, Ok(())) {
                continue;
            }

            tracing::error!("Couldn't send refresh message. Exiting refresh loop.");
        }
    });

    Box::new(rx)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InternalPreferences {
    pub friends_api_usage: Option<FriendsAPIUsage>,
    pub tf2_directory: Option<String>,
    pub rcon_password: Option<String>,
    pub steam_api_key: Option<String>,
    pub masterbase_key: Option<String>,
    pub masterbase_host: Option<String>,
    pub rcon_port: Option<u16>,
    pub dumb_autokick: Option<bool>,
    pub tos_agreement_date: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Preferences {
    pub internal: Option<InternalPreferences>,
    pub external: Option<serde_json::Value>,
}

impl Message<MACState> for Preferences {
    fn update_state(self, state: &mut MACState) {
        if let Some(internal) = self.internal {
            if let Some(tf2_dir) = internal.tf2_directory {
                let path: PathBuf = tf2_dir.into();
                state.settings.set_tf2_directory(path);
            }
            if let Some(rcon_pwd) = internal.rcon_password {
                state.settings.set_rcon_password(rcon_pwd);
            }
            if let Some(rcon_port) = internal.rcon_port {
                state.settings.set_rcon_port(rcon_port);
            }
            if let Some(steam_api_key) = internal.steam_api_key {
                state.settings.set_steam_api_key(steam_api_key);
            }
            if let Some(friends_api_usage) = internal.friends_api_usage {
                state.settings.set_friends_api_usage(friends_api_usage);
            }
            if let Some(masterbase_key) = internal.masterbase_key {
                state.settings.set_masterbase_key(masterbase_key);
            }
            if let Some(masterbase_host) = internal.masterbase_host {
                state.settings.set_masterbase_host(masterbase_host);
            }
            if let Some(autokick) = internal.dumb_autokick {
                state.settings.set_autokick_bots(autokick);
            }

            if let Some(tos_agreement_date) = internal.tos_agreement_date {
                if tos_agreement_date.is_empty() {
                    state.settings.set_tos_agreement_date(None);
                } else {
                    match DateTime::parse_from_rfc3339(&tos_agreement_date) {
                        Ok(date) => state.settings.set_tos_agreement_date(Some(date.to_utc())),
                        Err(e) => {
                            tracing::error!("Failed to set date of agreement to TOS ({tos_agreement_date}): {e}");
                        }
                    }
                }
            }
        }

        if let Some(external) = self.external {
            state.settings.update_external_preferences(external);
        }

        state.settings.save_ok();
    }
}

pub struct GitHubVersionLookup {
    pub tx: UnboundedSender<String>,
}

impl Message<MACState> for GitHubVersionLookup {
    fn update_state(self, _: &mut MACState) {}
}
pub struct GitHubVersionResponse {
    pub latest_version: String,
    pub tx: UnboundedSender<String>,
}

impl Message<MACState> for GitHubVersionResponse {
    fn update_state(self, _: &mut MACState) {}
}

pub struct GitHubVersionHandler;
impl Default for GitHubVersionHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl GitHubVersionHandler {
    #[must_use] pub fn new() -> Self {
        Self
    }
}

impl<IM, OM> MessageHandler<MACState, IM, OM> for GitHubVersionHandler
where
    IM: Is<GitHubVersionLookup>,
    OM: Is<GitHubVersionResponse>,
{
    fn handle_message(&mut self, _: &MACState, message: &IM) -> Option<event_loop::Handled<OM>> {
        let tx;
        if let Some(msg) = try_get::<GitHubVersionLookup>(message) {
            tx = msg.tx.clone();
            tracing::debug!("Attempting to fetch latest version from GitHub");
        } else {
            return None;
        }
        Handled::future(async move {
            let endpoint = format!("https://api.github.com/repos/{UPDATE_REPO}/releases");
            let response = match reqwest::Client::new()
                .get(endpoint)
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", "reqwest")
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    // TODO: prevent this error from being spammed when connection dies.
                    tracing::error!("Failed to get github release info: {:?}", e);
                    return None;
                }
            };

            let response_json: Value = response
                .json()
                .await
                .expect("Failed to parse github release info");
            let default = &vec![];
            let latest_release_json =
                response_json
                    .as_array()
                    .unwrap_or(default)
                    .iter()
                    .find(|r| {
                        r.as_object()
                            .unwrap()
                            .get("draft")
                            .is_some_and(|d| d.as_bool().is_some_and(|d| !d))
                    });

            let latest_release_json = match latest_release_json {
                Some(lr) => lr.to_owned(),
                None => {
                    return Some(OM::from(GitHubVersionResponse {
                        latest_version: MAC_VERSION.to_owned(),
                        tx,
                    }));
                }
            };

            let latest_release = match latest_release_json.get("tag_name") {
                Some(lr) => lr.as_str(),
                None => {
                    return Some(OM::from(GitHubVersionResponse {
                        latest_version: MAC_VERSION.to_owned(),
                        tx,
                    }));
                }
            };

            let broadcast_response: GitHubVersionResponse = GitHubVersionResponse {
                latest_version: latest_release.unwrap_or(MAC_VERSION).to_string(),
                tx,
            };

            Some(OM::from(broadcast_response))
        })
    }
}
