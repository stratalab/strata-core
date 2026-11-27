use super::support::Command;

#[path = "command_cases/admin_branch.rs"]
mod admin_branch;
#[path = "command_cases/arrow.rs"]
mod arrow;
#[path = "command_cases/event.rs"]
mod event;
#[path = "command_cases/graph.rs"]
mod graph;
#[path = "command_cases/hub.rs"]
mod hub;
#[path = "command_cases/json.rs"]
mod json;
#[path = "command_cases/kv.rs"]
mod kv;
#[path = "command_cases/vector.rs"]
mod vector;

use self::admin_branch::*;
use self::arrow::*;
use self::event::*;
use self::graph::*;
use self::hub::*;
use self::json::*;
use self::kv::*;
use self::vector::*;

pub(super) fn all_commands() -> Vec<Command> {
    let mut commands = admin_space_commands();
    commands.extend(hub_commands());
    commands.extend(branch_commands());
    commands.extend(kv_commands());
    commands.extend(json_commands());
    commands.extend(vector_commands());
    commands.extend(event_commands());
    commands.extend(graph_commands());
    commands.extend(arrow_commands());
    commands
}

pub(super) fn command_round_trip_cases() -> Vec<Command> {
    let mut commands = all_commands();
    commands.extend(admin_space_round_trip_edge_commands());
    commands.extend(kv_round_trip_edge_commands());
    commands.extend(json_round_trip_edge_commands());
    commands.extend(vector_round_trip_edge_commands());
    commands.extend(event_round_trip_edge_commands());
    commands.extend(graph_round_trip_edge_commands());
    commands.extend(graph_binding_target_round_trip_commands());
    commands.extend(arrow_round_trip_edge_commands());
    commands
}
