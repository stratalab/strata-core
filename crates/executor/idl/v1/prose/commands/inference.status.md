---
summary: Report what this binary can do before anything is attempted.
mcp_description: Use this when the user asks whether inference will work, which providers or models are available, or why an inference command failed.
---

Reports the inference facts that are knowable up front: whether this build can execute local models and download them, which providers are compiled in, which of those have an API key and where it was found, and how many catalogued models are already on disk.

Released binaries ship the cloud providers and leave local model execution out, so `local_execution` is false in them and every catalogued local model is unavailable until `strata inference install-local` adds it; `local_remedy` says so. Knowing that from `status` is the point: previously the only way to find out was to run an operation and read the failure.

`key_source` names where a key was read from: the environment variable, or the config file's path when the key was set with `strata config set <provider>.api_key`. The runtime asks the environment first and the config file second, and reports the one that answered, so `key_source` is exactly what a `generate` or `embed` call would use. The key itself is never returned.

The model directory is shared by every database on the machine, so a model downloaded once is available to all of them.
