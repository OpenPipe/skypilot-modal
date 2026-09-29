"""Absolute deadlines must not slide when a Pod starts late."""
# pylint: disable=protected-access,redefined-outer-name
import datetime
import json
from unittest import mock

from kubernetes import client
import pytest

from sky.provision import common as provision_common
from sky.provision import constants
from sky.provision.kubernetes import _pod_deadline
from sky.provision.kubernetes import config as config_lib
from sky.provision.kubernetes import instance


def pod(name='owned', uid='uid-1', start=140, duration=100, grace=30):
    return client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=name,
            uid=uid,
            resource_version='7',
            annotations={_pod_deadline.ANNOTATION: '200.75'}),
        spec=client.V1PodSpec(containers=[],
                              active_deadline_seconds=duration,
                              termination_grace_period_seconds=grace),
        status=client.V1PodStatus(start_time=(datetime.datetime.fromtimestamp(
            start, datetime.timezone.utc) if start is not None else None)))


@pytest.fixture(autouse=True)
def clock():
    with mock.patch.object(_pod_deadline.time, 'time',
                           return_value=150) as value:
        yield value


def test_delayed_start_tightens_with_grace_and_identity_preconditions():
    value = pod()
    deadlines = _pod_deadline.PodDeadlines([value])
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:
        assert deadlines.reconcile('ns', 'ctx', [value])
    api.assert_called_once_with('ctx')
    call = api.return_value.patch_namespaced_pod.call_args
    assert call.args == ('owned', 'ns')
    assert call.kwargs['body'] == [
        {
            'op': 'test',
            'path': '/metadata/uid',
            'value': 'uid-1'
        },
        {
            'op': 'test',
            'path': '/metadata/resourceVersion',
            'value': '7'
        },
        {
            'op': 'add',
            'path': '/spec/activeDeadlineSeconds',
            'value': 30
        },
    ]
    assert call.kwargs['_request_timeout'] == (3, 10)
    assert value.status.start_time.timestamp() + 30 + 30 < 200.75


@pytest.mark.parametrize('duration', [1, 7, 30])
def test_never_lengthens_a_shorter_limit(duration):
    value = pod(duration=duration)
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:
        assert _pod_deadline.PodDeadlines([value
                                          ]).reconcile('ns', 'ctx', [value])
    api.assert_not_called()


def test_pending_is_unestablished_and_unrelated_pods_are_not_touched():
    expected = pod(start=None)
    deadlines = _pod_deadline.PodDeadlines([expected])
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:
        assert not deadlines.reconcile('ns', 'ctx',
                                       [expected, pod(name='peer')])
    api.assert_not_called()


def test_replacement_uid_is_rejected_before_patch():
    deadlines = _pod_deadline.PodDeadlines([pod()])
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:
        with pytest.raises(config_lib.KubernetesError, match='identity'):
            deadlines.reconcile('ns', 'ctx', [pod(uid='replacement')])
    api.assert_not_called()


@pytest.mark.parametrize('status', [409, 422])
def test_api_conflict_retries_are_bounded(status):
    value = pod()
    deadlines = _pod_deadline.PodDeadlines([value])
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:
        api.return_value.patch_namespaced_pod.side_effect = client.ApiException(
            status)
        for _ in range(4):
            with pytest.raises(_pod_deadline.RetryPatch):
                deadlines.reconcile('ns', 'ctx', [value])
        with pytest.raises(client.ApiException):
            deadlines.reconcile('ns', 'ctx', [value])


def test_real_kubernetes_client_sends_identity_tests_as_json_patch(monkeypatch):
    value = pod()
    api = client.CoreV1Api()
    monkeypatch.setattr(_pod_deadline.kubernetes, 'core_api', lambda _: api)
    with mock.patch.object(api.api_client, 'call_api') as request:
        deadlines = _pod_deadline.PodDeadlines([value])
        assert deadlines.reconcile('ns', 'ctx', [value])
    assert request.call_args.args[4][
        'Content-Type'] == 'application/json-patch+json'
    assert request.call_args.kwargs['body'][0] == {
        'op': 'test',
        'path': '/metadata/uid',
        'value': 'uid-1'
    }


@pytest.mark.parametrize('raw',
                         ['nan', 'inf', '-1', 'bad', '0', '149', 201, True])
def test_invalid_or_expired_annotations_fail(raw):
    with pytest.raises(config_lib.KubernetesError):
        _pod_deadline.parse({_pod_deadline.ANNOTATION: raw})


def test_pending_deadline_does_not_wait_for_provision_timeout(clock):
    deadlines = _pod_deadline.PodDeadlines([pod(start=None)])
    clock.return_value = 201
    with pytest.raises(config_lib.KubernetesError, match='expired'):
        deadlines.check()


def test_insufficient_grace_budget_fails_without_widening_lifetime():
    value = pod(start=190)
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:
        with pytest.raises(config_lib.KubernetesError, match='grace'):
            _pod_deadline.PodDeadlines([value]).reconcile('ns', 'ctx', [value])
    api.assert_not_called()


def test_late_observation_cannot_spend_termination_grace(clock):
    value = pod()
    deadlines = _pod_deadline.PodDeadlines([value])
    clock.return_value = 190
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:
        with pytest.raises(config_lib.KubernetesError, match='wall time'):
            deadlines.reconcile('ns', 'ctx', [value])
    api.assert_not_called()


def test_patch_latency_cannot_be_reported_as_grace_safe(clock):
    value = pod()
    deadlines = _pod_deadline.PodDeadlines([value])
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:

        def delayed_patch(*args, **kwargs):
            del args, kwargs
            clock.return_value = 190

        api.return_value.patch_namespaced_pod.side_effect = delayed_patch
        with pytest.raises(config_lib.KubernetesError, match='wall time'):
            deadlines.reconcile('ns', 'ctx', [value])


def test_no_annotation_leaves_existing_pods_unchanged():
    value = pod()
    value.metadata.annotations = {}
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api') as api:
        deadlines = _pod_deadline.PodDeadlines([value])
        assert deadlines.reconcile('ns', 'ctx', [value])
    api.assert_not_called()


@pytest.mark.parametrize('phase', ['schedule', 'run'])
@pytest.mark.parametrize('conflict', [None, 409, 422])
def test_provision_poll_tightens_original_pod_before_returning(
        phase, conflict, monkeypatch):
    original = pod(start=None)
    observed = pod()
    for value in (original, observed):
        value.metadata.labels = {constants.TAG_SKYPILOT_CLUSTER_NAME: 'cluster'}
        value.spec.node_name = 'node'
        value.status.phase = 'Running'
    api = mock.MagicMock()
    api.list_namespaced_pod.return_value.items = [observed]
    if conflict is not None:
        api.patch_namespaced_pod.side_effect = [
            client.ApiException(conflict), None
        ]
    monkeypatch.setattr(instance.kubernetes, 'core_api', lambda _: api)
    monkeypatch.setattr(instance.time, 'sleep', lambda _: None)
    monkeypatch.setattr(instance.skypilot_config, 'get_effective_region_config',
                        lambda **kwargs: kwargs['default_value'])
    monkeypatch.setattr(instance.subprocess_utils, 'run_in_parallel',
                        lambda fn, values, n: [(True, None) for _ in values])
    if phase == 'schedule':
        instance._wait_for_pods_to_schedule(
            namespace='ns',
            context='ctx',
            new_nodes=[original],
            timeout=10,
            cluster_name='cluster',
            create_pods_start=observed.status.start_time)
    else:
        instance._wait_for_pods_to_run('ns', 'ctx', 'cluster', [original])
    patch = api.patch_namespaced_pod.call_args.kwargs['body']
    assert patch[-1]['value'] == 30
    assert patch[0]['value'] == original.metadata.uid
    assert api.list_namespaced_pod.call_count == (1 if conflict is None else 2)


@pytest.mark.parametrize('reason', ['expired', 'high availability'])
def test_annotation_rejects_before_provider_or_state_mutation(
        monkeypatch, reason):
    config = provision_common.ProvisionConfig(
        provider_config={},
        authentication_config={},
        docker_config={},
        node_config={
            'metadata': {
                'annotations': {
                    _pod_deadline.ANNOTATION: '149' if reason == 'expired' else
                                              '200.75'
                }
            }
        },
        count=1,
        tags={},
        resume_stopped_nodes=False,
        ports_to_open_on_launch=None)
    if reason == 'high availability':
        config.node_config['deployment_spec'] = {}
    monkeypatch.setattr(instance.kubernetes_utils, 'get_namespace_from_config',
                        lambda _: 'ns')
    monkeypatch.setattr(instance.kubernetes_utils,
                        'get_control_context_from_config', lambda _: 'ctx')
    with mock.patch.object(instance.kubernetes, 'core_api') as api, \
            mock.patch.object(instance.global_user_state,
                              'record_launch_milestone_for_cluster') as record:
        with pytest.raises(config_lib.KubernetesError, match=reason):
            instance._create_pods('region', 'cluster', 'cluster', config)
    api.assert_not_called()
    record.assert_not_called()


def test_multiple_pods_require_each_original_identity():
    first, second = pod(), pod(name='worker', uid='uid-2', start=None)
    deadlines = _pod_deadline.PodDeadlines([first, second])
    with mock.patch.object(_pod_deadline.kubernetes, 'core_api'):
        assert not deadlines.reconcile('ns', 'ctx', [first, second])
        second.status.start_time = first.status.start_time
        assert deadlines.reconcile('ns', 'ctx', [first, second])


@pytest.mark.parametrize('retry', [False, True])
def test_create_and_apparmor_retry_recheck_cutoff(clock, retry):
    apparmor_key = 'container.apparmor.security.beta.kubernetes.io/ray-node'
    spec = {
        'metadata': {
            'annotations': {
                _pod_deadline.ANNOTATION: '200.75',
                apparmor_key: 'unconfined'
            }
        }
    }
    _pod_deadline.parse(spec['metadata']['annotations'])
    with mock.patch.object(instance.kubernetes, 'core_api') as api:
        if retry:

            def forbidden(*args, **kwargs):
                del args, kwargs
                clock.return_value = 210
                error = client.ApiException(422)
                error.body = json.dumps(
                    {'message': 'FieldValueForbidden AppArmorProfile: nil'})
                raise error

            api.return_value.create_namespaced_pod.side_effect = forbidden
        else:
            clock.return_value = 210
        with pytest.raises(config_lib.KubernetesError, match='expired'):
            instance._create_namespaced_pod_with_retries('ns', spec, 'ctx')
    assert api.return_value.create_namespaced_pod.call_count == int(retry)
