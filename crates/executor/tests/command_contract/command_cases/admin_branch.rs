use crate::support::*;

pub(super) fn admin_space_commands() -> Vec<Command> {
    vec![
        Command::Ping {},
        Command::Info { branch: None },
        Command::Health {
            branch: Some("feature".to_owned()),
        },
        Command::Metrics { branch: None },
        Command::Describe {
            branch: Some("feature".to_owned()),
            space: Some("analytics".to_owned()),
        },
        Command::ConfigGet {},
        Command::ConfigureGetKey {
            key: "target".to_owned(),
        },
        Command::SpaceList { branch: None },
        Command::SpaceCreate {
            branch: Some("feature".to_owned()),
            space: "tenant_a".to_owned(),
        },
        Command::SpaceExists {
            branch: None,
            space: "tenant_a".to_owned(),
        },
        Command::SpaceDelete {
            branch: Some("feature".to_owned()),
            space: "tenant_a".to_owned(),
            force: true,
        },
    ]
}

pub(super) fn admin_space_round_trip_edge_commands() -> Vec<Command> {
    vec![
        Command::Info {
            branch: Some("default".to_owned()),
        },
        Command::SpaceList {
            branch: Some("feature".to_owned()),
        },
        Command::SpaceDelete {
            branch: None,
            space: "tenant_b".to_owned(),
            force: false,
        },
    ]
}

pub(super) fn branch_commands() -> Vec<Command> {
    vec![
        Command::BranchList {},
        Command::BranchGet {
            branch: "main".to_owned(),
        },
        Command::BranchCreate {
            branch: "scratch".to_owned(),
        },
        Command::BranchForkCurrent {
            source: "default".to_owned(),
            branch: "feature".to_owned(),
        },
        Command::BranchForkAtVersion {
            source: "default".to_owned(),
            branch: "by-version".to_owned(),
            version: 7,
        },
        Command::BranchForkAtTimestamp {
            source: "default".to_owned(),
            branch: "by-time".to_owned(),
            timestamp: 99,
        },
        Command::BranchDelete {
            branch: "scratch".to_owned(),
        },
    ]
}
