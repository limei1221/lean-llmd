"""Day 6 - the EPP as Kubernetes objects: RBAC, Envoy's config, one Pod with two containers.

Standalone mode, as in llm-d-router's ``llm-d-router-standalone`` chart:
Envoy runs next to the EPP in the same Pod and reaches it on localhost.
``lean-llmd render epp`` prints these as YAML.
"""

from typing import Any

import yaml

Obj = dict[str, Any]

EPP_IMAGE = "lean-llmd:dev"
ENVOY_IMAGE = "docker.io/envoyproxy/envoy:distroless-v1.33.2"
EXT_PROC_PORT = 9002
PROXY_PORT = 8081
ENVOY_READY_PORT = 19001
ENVOY_ADMIN_PORT = 19000
DESTINATION_HEADER = "x-gateway-destination-endpoint"

# Given, not exercises: read them, don't retype them. make_stubs copies these verbatim.
__given__ = ("_typed", "envoy_config")


def _typed(type_name: str, **fields: Any) -> Obj:
    return {"@type": f"type.googleapis.com/{type_name}", **fields}


def envoy_config(ext_proc_port: int = EXT_PROC_PORT, proxy_port: int = PROXY_PORT) -> Obj:
    """Envoy's bootstrap config as a dict.

    - Listener ``proxy`` on ``proxy_port``: an HTTP connection manager whose
      only route sends everything to ``original_destination_cluster``, with
      the ``ext_proc`` filter before the router filter.
    - ``ext_proc``: gRPC to cluster ``ext_proc``; send request headers, buffer
      the request body, skip everything on the response; ``message_timeout``
      120s (a request may wait in day 10's flow-control queue); fail closed.
    - ``original_destination_cluster``: type ``ORIGINAL_DST`` reading the
      upstream address from the ``x-gateway-destination-endpoint`` header.
    - ``ext_proc`` cluster: static ``127.0.0.1:ext_proc_port`` over HTTP/2.
    - A ``ready`` listener on ``ENVOY_READY_PORT`` answering ``/ready`` (for
      the readiness probe) and an admin interface on localhost.
    """
    http_filters = [
        {
            "name": "envoy.filters.http.ext_proc",
            "typed_config": _typed(
                "envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor",
                grpc_service={"envoy_grpc": {"cluster_name": "ext_proc"}, "timeout": "10s"},
                failure_mode_allow=False,
                message_timeout="120s",
                processing_mode={
                    "request_header_mode": "SEND",
                    "response_header_mode": "SKIP",
                    "request_body_mode": "BUFFERED",
                    "response_body_mode": "NONE",
                    "request_trailer_mode": "SKIP",
                    "response_trailer_mode": "SKIP",
                },
            ),
        },
        {
            "name": "envoy.filters.http.router",
            "typed_config": _typed("envoy.extensions.filters.http.router.v3.Router"),
        },
    ]
    proxy_listener = {
        "name": "proxy",
        "address": {"socket_address": {"address": "0.0.0.0", "port_value": proxy_port}},
        "filter_chains": [
            {
                "filters": [
                    {
                        "name": "envoy.filters.network.http_connection_manager",
                        "typed_config": _typed(
                            "envoy.extensions.filters.network.http_connection_manager.v3."
                            "HttpConnectionManager",
                            stat_prefix="proxy",
                            route_config={
                                "name": "proxy",
                                "virtual_hosts": [
                                    {
                                        "name": "all",
                                        "domains": ["*"],
                                        "routes": [
                                            {
                                                "match": {"prefix": "/"},
                                                "route": {
                                                    "cluster": "original_destination_cluster",
                                                    "timeout": "300s",
                                                },
                                            }
                                        ],
                                    }
                                ],
                            },
                            http_filters=http_filters,
                        ),
                    }
                ]
            }
        ],
    }
    ready_listener = {
        "name": "ready",
        "address": {"socket_address": {"address": "0.0.0.0", "port_value": ENVOY_READY_PORT}},
        "filter_chains": [
            {
                "filters": [
                    {
                        "name": "envoy.filters.network.http_connection_manager",
                        "typed_config": _typed(
                            "envoy.extensions.filters.network.http_connection_manager.v3."
                            "HttpConnectionManager",
                            stat_prefix="ready",
                            route_config={"name": "ready", "virtual_hosts": []},
                            http_filters=[
                                {
                                    "name": "envoy.filters.http.health_check",
                                    "typed_config": _typed(
                                        "envoy.extensions.filters.http.health_check.v3.HealthCheck",
                                        pass_through_mode=False,
                                        headers=[
                                            {"name": ":path", "string_match": {"exact": "/ready"}}
                                        ],
                                    ),
                                },
                                {
                                    "name": "envoy.filters.http.router",
                                    "typed_config": _typed(
                                        "envoy.extensions.filters.http.router.v3.Router"
                                    ),
                                },
                            ],
                        ),
                    }
                ]
            }
        ],
    }
    clusters = [
        {
            "name": "original_destination_cluster",
            "type": "ORIGINAL_DST",
            "connect_timeout": "5s",
            "lb_policy": "CLUSTER_PROVIDED",
            "original_dst_lb_config": {
                "use_http_header": True,
                "http_header_name": DESTINATION_HEADER,
            },
        },
        {
            "name": "ext_proc",
            "type": "STATIC",
            "connect_timeout": "1s",
            "typed_extension_protocol_options": {
                "envoy.extensions.upstreams.http.v3.HttpProtocolOptions": _typed(
                    "envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
                    explicit_http_config={"http2_protocol_options": {}},
                )
            },
            "load_assignment": {
                "cluster_name": "ext_proc",
                "endpoints": [
                    {
                        "lb_endpoints": [
                            {
                                "endpoint": {
                                    "address": {
                                        "socket_address": {
                                            "address": "127.0.0.1",
                                            "port_value": ext_proc_port,
                                        }
                                    }
                                }
                            }
                        ]
                    }
                ],
            },
        },
    ]
    return {
        "admin": {
            "address": {"socket_address": {"address": "127.0.0.1", "port_value": ENVOY_ADMIN_PORT}}
        },
        "static_resources": {"listeners": [proxy_listener, ready_listener], "clusters": clusters},
    }


def epp_rbac(name: str, namespace: str) -> list[Obj]:
    """A ServiceAccount, a Role and a RoleBinding named ``name``.

    The Role may only ``get``, ``list`` and ``watch`` Pods (core group) and
    InferencePools (``inference.networking.k8s.io``), in this namespace.
    """
    read = ["get", "list", "watch"]
    return [
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": name, "namespace": namespace},
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": name, "namespace": namespace},
            "rules": [
                {"apiGroups": [""], "resources": ["pods"], "verbs": read},
                {
                    "apiGroups": ["inference.networking.k8s.io"],
                    "resources": ["inferencepools"],
                    "verbs": read,
                },
            ],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": name, "namespace": namespace},
            "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": namespace}],
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
        },
    ]


def envoy_config_map(name: str, namespace: str) -> Obj:
    """A ConfigMap ``<name>-envoy`` holding ``envoy.yaml`` (``envoy_config()`` as YAML)."""
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": f"{name}-envoy", "namespace": namespace},
        "data": {"envoy.yaml": yaml.safe_dump(envoy_config(), sort_keys=False)},
    }


def epp_deployment(
    name: str,
    namespace: str,
    pool_name: str,
    image: str = EPP_IMAGE,
    package: str = "lean_llmd",
    extra_args: tuple[str, ...] = (),
) -> Obj:
    """One replica; Pods labelled ``app: <name>``, running as ServiceAccount ``name``.

    - Container ``epp``: ``python -m <package>.epp --pool-name <pool_name>
      --pool-namespace <namespace> *extra_args``, port ``grpc`` 9002, with a
      readiness probe using Kubernetes' native gRPC health check.
    - Container ``envoy``: ``ENVOY_IMAGE`` with ``-c /etc/envoy/envoy.yaml``
      mounted from the ConfigMap, port ``http`` 8081, readiness on
      ``/ready`` at ``ENVOY_READY_PORT``.
    - Small requests (100m CPU / 128Mi for epp, 50m / 64Mi for envoy).
    """
    labels = {"app": name}
    epp: Obj = {
        "name": "epp",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["python", "-m", f"{package}.epp"],
        "args": ["--pool-name", pool_name, "--pool-namespace", namespace, *extra_args],
        "ports": [{"name": "grpc", "containerPort": EXT_PROC_PORT}],
        "readinessProbe": {"grpc": {"port": EXT_PROC_PORT}, "periodSeconds": 2},
        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "1Gi"}},
    }
    envoy: Obj = {
        "name": "envoy",
        "image": ENVOY_IMAGE,
        "imagePullPolicy": "IfNotPresent",
        "args": ["-c", "/etc/envoy/envoy.yaml", "--log-level", "warn"],
        "ports": [{"name": "http", "containerPort": PROXY_PORT}],
        "readinessProbe": {
            "httpGet": {"path": "/ready", "port": ENVOY_READY_PORT},
            "periodSeconds": 2,
        },
        "volumeMounts": [{"name": "envoy", "mountPath": "/etc/envoy"}],
        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"memory": "512Mi"}},
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": namespace, "labels": labels},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "serviceAccountName": name,
                    "containers": [epp, envoy],
                    "volumes": [{"name": "envoy", "configMap": {"name": f"{name}-envoy"}}],
                },
            },
        },
    }


def epp_service(name: str, namespace: str) -> Obj:
    """ClusterIP Service ``name``: port 80 to Envoy's ``http``, 9002 to the EPP's ``grpc``."""
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "selector": {"app": name},
            "ports": [
                {"name": "http", "port": 80, "targetPort": "http"},
                {"name": "grpc", "port": EXT_PROC_PORT, "targetPort": "grpc"},
            ],
        },
    }


def epp_objects(
    name: str,
    namespace: str,
    pool_name: str,
    image: str = EPP_IMAGE,
    package: str = "lean_llmd",
    extra_args: tuple[str, ...] = (),
) -> list[Obj]:
    """Everything above, in apply order."""
    return [
        *epp_rbac(name, namespace),
        envoy_config_map(name, namespace),
        epp_deployment(name, namespace, pool_name, image, package, extra_args),
        epp_service(name, namespace),
    ]
