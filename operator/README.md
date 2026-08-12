# operator

## Description
The Control Plane is implemented as a Kubernetes Operator using controller-runtime.

It converts Kubernetes declarative resources into runtime routing configuration and distributes configuration changes to Data Plane instances through xDS.

## Getting Started

### Prerequisites
- go version v1.26.5
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Test on the kind environment
**NOTE:** You need to close Webhooks feature  when test the operator. 
Because the command not generate certificates when u test.

**Run the Operator int the kind**
```sh
make run ENABLE_WEBHOOKS=false
```
### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/operator:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/operator:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/operator/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v2-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.


## Interaction Diagram[Details]
```mermaid
sequenceDiagram

    autonumber

    participant User as User / GitOps
    participant API as Kubernetes API Server
    participant CC as config_controller
    participant TC as tenantpipeline_controller
    participant EC as endpointslice_controller
    participant Cache as xdsController<br/>XDS Cache
    participant Deb as Debouncer
    participant Bus as PushEventDispatchBus
    participant XDS as xdsServer
    participant ClientState as ClientState
    participant Version as versionCache
    participant DP as xdsClient
    participant Runtime as RuntimeBackend

    User->>API: Create / Update / Delete Config CR

    API-->>CC: Config Watch Event
    CC->>CC: Reconcile()

    alt Create / Update
        CC->>Cache: Upsert DTO → XDS Resource
    else Delete
        CC->>Cache: Delete namespaced resource
    end

    CC->>Deb: ReconcileEvent

    Note over Deb: Collect events during Ticker Period<br/>Deduplicate by map[string]struct{}

    Deb->>Deb: Deduplicate / Aggregate
    Deb->>Bus: XDSPushEvent

    Bus->>XDS: Handle Push Event

    XDS->>Cache: Create Snapshot
    Cache-->>XDS: xdsSnapshot

    XDS->>ClientState: Find active xDS clients

    XDS->>XDS: PushDeltaResources()

    alt Delta xDS
        XDS->>DP: Delta Resources
    else State of the World
        XDS->>DP: Full Snapshot
    end

    DP->>Runtime: COW Update RuntimeBackend

    DP-->>XDS: ACK(version / nonce)

    XDS->>Version: Save successful snapshot

    Note over Version: Used for NACK recovery

    alt NACK
        DP-->>XDS: NACK(version / nonce)
        XDS->>Version: Load last successful snapshot
        Version-->>XDS: Previous Valid Snapshot
        XDS->>DP: Re-send valid configuration
    end

    Note over EC: EndpointSlice changes are<br>independent runtime events

    API-->>EC: EndpointSlice Watch Event
    EC->>Cache: Update Endpoint Resource
    EC->>Deb: ReconcileEvent
```


## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project
> Notice: The basic project skeleton is generated by Kubebuilder.
> All core reconciliation business logic is independently developed by duuuuu17.

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

MIT License

Copyright (c) 2026 duuuuu17

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

