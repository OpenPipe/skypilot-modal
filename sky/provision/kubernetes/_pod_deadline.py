"""Tighten opted-in Pod lifetimes after Kubernetes assigns their start times."""
import math
import time
from typing import Dict, List, Mapping, Optional, Tuple, TYPE_CHECKING

from sky.adaptors import kubernetes
from sky.provision.kubernetes import config as config_lib

if TYPE_CHECKING:
    from kubernetes.client import V1Pod

ANNOTATION = 'skypilot-absolute-deadline'


class RetryPatch(Exception):
    """Re-observe a Pod after an optimistic concurrency conflict."""


def parse(annotations: Optional[Mapping[str, str]]) -> Optional[float]:
    """The annotation is an absolute Unix time, including termination grace."""
    if not annotations or ANNOTATION not in annotations:
        return None
    if not isinstance(annotations[ANNOTATION], str):
        raise config_lib.KubernetesError(f'{ANNOTATION} must be a string')
    try:
        deadline = float(annotations[ANNOTATION])
    except (TypeError, ValueError) as error:
        raise config_lib.KubernetesError(
            f'{ANNOTATION} must be a finite positive Unix timestamp') from error
    if not math.isfinite(deadline) or deadline <= 0:
        raise config_lib.KubernetesError(
            f'{ANNOTATION} must be a finite positive Unix timestamp')
    if time.time() >= deadline:
        raise config_lib.KubernetesError(
            'The absolute Pod deadline has expired')
    return deadline


class PodDeadlines:
    """Bind deadline reconciliation to the original returned Pod identities."""

    def __init__(self, pods: List['V1Pod']) -> None:
        self.expected: Dict[str, Tuple[str, float]] = {}
        self._grace_deadline = math.inf
        self._conflicts: Dict[str, int] = {}
        for pod in pods:
            deadline = parse(getattr(pod.metadata, 'annotations', None))
            if deadline is not None:
                if not pod.metadata.uid:
                    raise config_lib.KubernetesError(
                        'Absolute Pod deadline requires an owned Pod UID')
                self.expected[pod.metadata.name] = (pod.metadata.uid, deadline)

    def check(self) -> None:
        if not self.expected:
            return
        now = time.time()
        if any(now >= deadline for _, deadline in self.expected.values()):
            raise config_lib.KubernetesError(
                'The absolute Pod deadline has expired')
        if now >= self._grace_deadline:
            raise config_lib.KubernetesError(
                'Insufficient wall time remains for Pod termination grace')

    def reconcile(self, namespace: str, context: Optional[str],
                  pods: List['V1Pod']) -> bool:
        self.check()
        established = set()
        for pod in pods:
            if pod.metadata.name not in self.expected:
                continue
            uid, deadline = self.expected[pod.metadata.name]
            if pod.metadata.uid != uid:
                raise config_lib.KubernetesError(
                    'Pod identity changed before deadline reconciliation')
            grace = pod.spec.termination_grace_period_seconds
            grace = 30 if grace is None else grace
            self._grace_deadline = min(self._grace_deadline, deadline - grace)
            self.check()
            if pod.status.start_time is None:
                continue
            limit = math.floor(deadline - pod.status.start_time.timestamp() -
                               grace)
            if limit < 1:
                raise config_lib.KubernetesError(
                    'Insufficient Pod lifetime remains for termination grace')
            current = pod.spec.active_deadline_seconds
            if current is None or current > limit:
                version = pod.metadata.resource_version
                if not version:
                    raise config_lib.KubernetesError(
                        'Pod resource version is required for its deadline')
                # Both tests prevent changing a replacement or racing a shorter
                # limit written by another controller after this observation.
                try:
                    kubernetes.core_api(context).patch_namespaced_pod(
                        pod.metadata.name,
                        namespace,
                        body=[
                            {
                                'op': 'test',
                                'path': '/metadata/uid',
                                'value': uid
                            },
                            {
                                'op': 'test',
                                'path': '/metadata/resourceVersion',
                                'value': version
                            },
                            {
                                'op': 'add',
                                'path': '/spec/activeDeadlineSeconds',
                                'value': limit
                            },
                        ],
                        _request_timeout=(3, 10))
                except kubernetes.api_exception() as error:
                    name = pod.metadata.name
                    self._conflicts[name] = self._conflicts.get(name, 0) + 1
                    if error.status not in (409,
                                            422) or self._conflicts[name] >= 5:
                        raise
                    raise RetryPatch from error
            self._conflicts.pop(pod.metadata.name, None)
            established.add(pod.metadata.name)
        self.check()
        return established == set(self.expected)
