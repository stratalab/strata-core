---
title: "branch commands"
description: "Command reference for the branch family."
source: strata-core@1.2.2
section: branch
---

# `branch` — command reference

| Command | Summary |
|---|---|
| [Create empty branch](/docs/branch/create) | Create a new empty root branch. |
| [Delete branch](/docs/branch/delete) | Delete an active branch and release its storage claims. |
| [Compare branches](/docs/branch/diff) | Compare two branches and report the entities that differ across every primitive. |
| [Fork branch from current head](/docs/branch/fork) | Fork a new branch from the current head of a source branch. |
| [Fork branch at timestamp](/docs/branch/fork_at_timestamp) | Fork a new branch from a retained source timestamp. |
| [Fork branch at version](/docs/branch/fork_at_version) | Fork a new branch from a retained source commit version. |
| [Read one branch](/docs/branch/get) | Read one branch summary by name. |
| [List branches](/docs/branch/list) | List active branches with their lineage facts. |
| [Promote branch](/docs/branch/merge) | Promote one branch's changes into another as a single atomic commit. |
| [Preview branch promotion](/docs/branch/preview) | Preview promoting one branch into another, reporting conflicts without mutating either branch. |
