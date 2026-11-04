# k8s

A sandbox runner that executes each request as one Kubernetes Job under a restricted security context.

Part of the [orchagent](https://github.com/urmzd/orchagent) workspace. Kept as its own module so importers of orchagent do not inherit client-go.

```sh
go get github.com/urmzd/orchagent/sandbox/runner/k8s
```

RBAC for the executor lives in [`manifests/`](manifests/).

## License

[Apache-2.0](LICENSE)
