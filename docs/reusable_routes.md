# Reusable API integrations

The following modules expose setup functions or router builders for applications
embedding Tangle in FastAPI. Applications supply authentication, database sessions,
external-service clients, and deployment configuration.

All module paths below are relative to `cloud_pipelines_backend`.

| Feature | Module and entry point |
| --- | --- |
| Saved pipelines, versions, execution, and saved-pipeline search | `user_pipelines.api_routes.setup_user_pipeline_routes` |
| Cron schedules and manual scheduled runs | `scheduling.pipelines.api_routes.setup_pipeline_schedule_routes` |
| Event-triggered pipeline subscriptions | `triggers.api_routes.setup_trigger_routes` |
| Quota groups, claims, and promotion | `quota.api_routes.setup_quota_group_routes` |
| Workspaces, projects, resources, and associated runs | `projects.api_routes.setup_project_routes` |
| Notices | `notices.api_routes.setup_notice_routes` |
| Elasticsearch indexing and search tools | `search.elasticsearch.elastic_search_api.setup_elastic_search_routes` |
| Published-component search | `search.published_components.api_routes.setup_component_search_routes` |
| Tangent instances, authentication proxy, OpenCode, and port forwarding | `tangent.*_routes.build_api_router` |
| Generic HTTP forwarding | `proxy_api_routes.setup_routes` |

## Integration requirements

Register the applicable `db_models` modules before database creation/migration.
Existing scheduler and emission databases also require their respective schema
migration functions; creating missing tables alone does not expand existing tables.
Start and stop `SchedulerService` with the application's lifecycle.

`UserPipelineService` accepts optional `RunHooks` for application-specific preparation
and run-created handling. Supply the configured service to saved-pipeline routes,
scheduler service, trigger routes, and the readiness handler's `StartPipelineRunSink`
when those execution paths need the same hooks.

Elasticsearch tools accept a client factory through `configure_elasticsearch` and
embedding configuration through `configure_embeddings`. Component search takes its
client factory and embedding-function getter directly. Credentials and provider
endpoints belong in the embedding application.

Tangent router builders accept Kubernetes clients and deployment inputs. Instance
creation requires an agent-container factory, volumes, and pod annotations. The
embedding application owns those choices and its user identity dependencies.

The HTTP proxy requires a trusted upstream URL, a caller-header allowlist, and an
authenticating dependency that supplies trusted outbound headers.

Emission production includes readiness and quota codecs. Applications can register
additional codecs using `emissions.producer.configure_kinds` before installing
listeners, and supply handlers and sinks to the dispatcher/consumer. Quota admission,
reconciliation, and background polling remain explicit worker integration steps.

See [saved pipelines](user_pipelines.md),
[pipeline schedules](../cloud_pipelines_backend/scheduling/pipelines/PIPELINE_SCHEDULER_USAGE.md),
[triggers](../cloud_pipelines_backend/triggers/TRIGGER_USAGE.md), and
[quota groups](../cloud_pipelines_backend/quota/QUOTA_GROUP_USAGE.md) for API details.
