# Tangle

The Tangle system helps users create and run ML experiments and production pipelines.
Any batch workflow that has beginning and end can be orchestrated via a pipeline.

To learn more about Tangle, visit <https://tangleml.com>

[![image](https://github.com/user-attachments/assets/0ce7ccc0-dad7-4f6a-8677-f2adcd83f558)](https://tangleml-tangle.hf.space/#/quick-start)

## Try Tangle

Tangle’s architecture allows it to run anywhere. We’re expanding the list of installation recipes.
At this moment Tangle can be used locally (using Docker/Podman launcher) or at HuggingFace (using HuggingFace Job launcher).

### Try at HuggingFace

The easiest way to try Tangle is via HuggingFace.

Go to [TangleML/tangle](https://tangleml-tangle.hf.space/#/quick-start) at HuggingFace and start building. Creating a pipeline does not require any registration, but to run your pipeline, you’ll need a HuggingFace account with [Pro](https://huggingface.co/pro) subscription ($9/month).

### Build a pipeline
Start with the sample XGBoost training pipeline or build a new one from scratch:

*   Drag components onto canvas
*   Connect components (outputs to inputs)
*   Configure task arguments
*   Submit pipeline for execution (requires login)
*   Monitor the pipeline run in real-time

### HuggingFace x Tangle integration

The Tangle team has deployed Tangle to HuggingFace Spaces as a multi-tenant service, which uses HF infrastructure for storage, compute, and authentication. 

The shared multi-tenant instance is live at <https://tangleml-tangle.hf.space> (or <https://huggingface.co/spaces/TangleML/tangle>).

Tangle’s multi-tenant architecture maintains a central tenant database (storing user IDs, access tokens, and orchestrator configs) plus individual per-tenant SQLite databases in the main TangleML/tangle HuggingFace Space persistent storage. Each user’s pipelines run via that user’s HuggingFace Jobs, and the execution logs and output artifacts are stored in the user's own private HuggingFace Dataset repo (user/tangle_data), with clickable links in the UI to both artifacts and HuggingFace Jobs. 

You can also deploy your own single-tenant instance. To do this, [duplicate](https://huggingface.co/spaces/TangleML/tangle?duplicate=true) the Tangle space to your HF account and provide a [HuggingFace token](https://huggingface.co/docs/hub/en/security-tokens). These single-tenant Tangle deployments store their database in your own HF Space persistent storage, giving you complete control and data isolation.

If you duplicate Tangle to an organization, you’ll get a single-tenant multi-user Tangle deployment for your team, where multiple team members can see each other’s pipeline runs and enjoy org-wide cache.

You can also deploy Tangle to other environments (local or cloud).

## Installation

### Try on local machine

1. Install [Docker](https://www.docker.com/get-started/) and [uv](https://docs.astral.sh/uv/getting-started/installation/).
2. Download the app code (needs to be done once):

```shell
git clone https://github.com/TangleML/tangle.git tangle/backend --branch stable
git clone https://github.com/TangleML/tangle-ui.git tangle/ui_build --branch stable_local_build
```

3. Start the app:

Linux and Mac OS:

```shell
cd tangle && backend/start_local.sh
```

Windows:

```bat
cd tangle && backend\start_local.cmd
```

4. Once the "start_local: Starting the orchestrator" message appears in the terminal, open the [http://localhost:8000](http://localhost:8000) URL in a Web browser and start use the app.
Click the "New Pipeline" button at the top to start building a new pipeline.

### Try in Google Cloud Shell (free)

[Google Cloud Shell](https://cloud.google.com/shell/) is free (50 hours per week) and needs a Google Cloud account.

1. Open [Google Cloud Shell](https://shell.cloud.google.com/?show=terminal) in a Web browser
2. Download the app code (needs to be done once):

```shell
git clone https://github.com/TangleML/tangle.git tangle/backend --branch stable
git clone https://github.com/TangleML/tangle-ui.git tangle/ui_build --branch stable_local_build
```

3. Start the app:

```shell
cd tangle && backend/start_local.sh
```

4. Once the "start_local: Starting the orchestrator", "View app at" messages appears in the terminal, open the <https://shell.cloud.google.com/devshell/proxy?port=8000> URL in another browser tab and start using the app.


## App features

*   Start building pipelines right away
    * Intuitive visual drag and drop interface
    * No registration required to build. You own your data.
*   Execute pipelines on your local machine or in Cloud
    * Easily install the app on local machine or deploy to cloud
    * Submit pipelines for execution with a single click.
    * Easily monitor all pipeline task executions, view the artifacts, read the logs.
*   Fast iteration
    * Clone any pipeline run and get a new editable pipeline
    * Create pipeline -> Submit run -> Monitor run -> Clone run -> Edit pipeline -> Submit run ...
*   Automatic execution caching and reuse
    * Save time and compute. Don't re-do what's done
    * Successful and even running executions are re-used from cache
*   Reproducibility
    * All your runs are kept forever (on your machine) - graph, logs, metadata
    * Re-run an old pipeline run with just two clicks (Clone pipeline, Submit run)
    * Containers and strict component versioning ensure reproducibility
*   Pipeline Components
    * Time-proven `ComponentSpec`/`component.yaml` format
    * A library of preloaded components
    * Fast-growing public component ecosystem
    * Add your own components (public or private)
    * Easy to create your own components manually or using the Cloud Pipelines SDK
    * Components can be written in [any language](https://github.com/Ark-kun/pipeline_components/tree/master/components/sample) (Python, Shell, R, Java, C#, etc).
    * Compatible with [Google Cloud Vertex AI Pipelines](https://cloud.google.com/vertex-ai/docs/pipelines/introduction) and [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/introduction/)
    * Lots of pre-built components on GitHub: [Ark-kun/pipeline_components](https://github.com/Ark-kun/pipeline_components/tree/master/components).


## Concepts

A **pipeline system** like Tangle orchestrates containerized command-line programs.
When pipeline system runs a pipeline, it executes an interconnected graph of containerized programs locally or remotely (e.g. in cloud), and facilitates the transfer of data between them.

A **pipeline** is a *graph* of interconnected component tasks.

A **component** describes a certain command-line program inside a container. Component specification describes its signature (inputs, outputs), metadata (name, description and annotations) and implementation which specifies which container image to use, which program to start and and how to connect the inputs and outputs to the program's command-line arguments.
Components can be written in any language. All Cloud Pipelines projects including Tangle supports arbitrary containers and arbitrary programs.

A **task** describes an instance of a component and specifies the input arguments for the component's inputs. Tasks are connected together into a graph by linking some upstream task outputs to some downstream task inputs.

The resulting *graph* of interconnected tasks is called a *pipeline*.
A pipeline can be submitted for *execution*. During the pipeline execution, the pipeline's tasks are executed (in parallel, if possible) and produce output artifacts that are passed to downstream tasks.

## Design

This backend consists of the API Server and the Orchestrator.

### API Server

The API Server receives API requests and accesses the database to fulfill them.
The API documentation can be accessed at [http://localhost:8000/docs](http://localhost:8000/docs).

## CORS Middleware

### Purpose

The CORS (Cross-Origin Resource Sharing) middleware allows web browsers to make requests to the Tangle API from different origins (domains). This is essential for:
- Local development (frontend on `localhost:3000`, backend on `localhost:8000`)
- Production deployments where the frontend and backend are on different domains
- Multi-tenant deployments with multiple frontend URLs

### How It Works

1. **Environment Variable Configuration**: Origins are specified in the `TANGLE_CORS_ALLOWED_ORIGINS` environment variable as a comma-separated list
2. **Request Validation**: For each incoming request, the middleware checks the `Origin` header from the browser
3. **Dynamic Response**: If the origin is in the allowed list, the server responds with `Access-Control-Allow-Origin` set to that specific origin
4. **Security**: Only pre-approved origins receive CORS headers, preventing unauthorized cross-origin access

### Orchestrator

The Orchestrator works independently from the API Server.
It launches container executions and facilitates data passing between executions.
The Orchestrator and the API Server communicate via the database.
The Orchestrator launches container tasks using a specified Launcher, communicating with it via abstract interface. Such flexibility helps support different container execution systems and cloud providers.

### Database

The backend uses SqlAlchemy to abstract the database access, so any database engine supported by SqlAlchemy can be used.
We officially support the Sqlite and MySQL databases.

![DB diagram](./docs/db_diagram.svg)

### Launchers

Launchers launch container executions on a local or remote computer.
Currently the following launchers are supported:

* Local Docker using local storage
* Local Kubernetes using local storage via HostPath volumes
* Google Cloud Kubernetes Engine using Google Cloud Storage

More launchers may be added in the future.

### Credits

This [Tangle Pipelines](https://github.com/TangleML/tangle) [backend](https://github.com/Cloud-Pipelines/backend) is created by [Alexey Volkov](https://github.com/Ark-kun) as part of the [Cloud Pipelines](https://github.com/Cloud-Pipelines) project. It's derived from the [Cloud Pipelines SDK](https://github.com/Cloud-Pipelines/sdk) orchestrator and uses parts of it under the hood.
