"""Tests for the SSH websocket proxy client."""

# pylint: disable=missing-class-docstring,protected-access

import asyncio

from sky.templates import websocket_proxy


class _ConnectionClosed(Exception):
    pass


class _Writer:

    def write(self, data: bytes) -> None:
        del data

    async def drain(self) -> None:
        pass


class _Socket:

    def __init__(self, receive_started: asyncio.Event,
                 close_receive: asyncio.Event) -> None:
        self._receive_started = receive_started
        self._close_receive = close_receive

    async def recv(self) -> bytes:
        self._receive_started.set()
        await self._close_receive.wait()
        raise _ConnectionClosed

    async def send(self, data: bytes) -> None:
        del data

    async def close(self) -> None:
        pass


def test_peer_close_cancels_open_stdin(monkeypatch) -> None:
    asyncio.run(_test_peer_close_cancels_open_stdin(monkeypatch))


async def _test_peer_close_cancels_open_stdin(monkeypatch) -> None:
    monkeypatch.setattr(websocket_proxy.websockets.exceptions,
                        'ConnectionClosed', _ConnectionClosed)
    read_started = asyncio.Event()
    read_cancelled = asyncio.Event()
    receive_started = asyncio.Event()
    close_receive = asyncio.Event()

    class _Reader:

        async def read(self, count: int) -> bytes:
            del count
            read_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                read_cancelled.set()
                raise

    closed = asyncio.Event()
    lock = asyncio.Lock()
    socket = _Socket(receive_started, close_receive)
    proxy = asyncio.create_task(
        websocket_proxy._run_until_proxy_closed(
            closed,
            (
                websocket_proxy.stdin_to_websocket(_Reader(), socket, False,
                                                   closed, lock),
                websocket_proxy.websocket_to_stdout(socket, _Writer(), False,
                                                    None, closed, lock),
            ),
            (websocket_proxy.latency_monitor(socket, None, closed, lock),),
        ))

    await asyncio.wait_for(read_started.wait(), 1)
    await asyncio.wait_for(receive_started.wait(), 1)
    await asyncio.sleep(0)
    assert not proxy.done()

    close_receive.set()
    await asyncio.wait_for(proxy, 1)

    assert closed.is_set()
    assert read_cancelled.is_set()


def test_stdin_eof_cancels_open_receive() -> None:
    asyncio.run(_test_stdin_eof_cancels_open_receive())


async def _test_stdin_eof_cancels_open_receive() -> None:
    receive_started = asyncio.Event()
    receive_cancelled = asyncio.Event()

    class _Reader:

        async def read(self, count: int) -> bytes:
            del count
            await receive_started.wait()
            return b''

    class _OpenSocket:

        async def recv(self) -> bytes:
            receive_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                receive_cancelled.set()
                raise

        async def send(self, data: bytes) -> None:
            del data

        async def close(self) -> None:
            pass

    closed = asyncio.Event()
    lock = asyncio.Lock()
    socket = _OpenSocket()
    await asyncio.wait_for(
        websocket_proxy._run_until_proxy_closed(
            closed,
            (
                websocket_proxy.stdin_to_websocket(_Reader(), socket, False,
                                                   closed, lock),
                websocket_proxy.websocket_to_stdout(socket, _Writer(), False,
                                                    None, closed, lock),
            ),
            (websocket_proxy.latency_monitor(socket, None, closed, lock),),
        ), 1)

    assert closed.is_set()
    assert receive_cancelled.is_set()


def test_caller_cancellation_reaps_proxy_tasks() -> None:
    asyncio.run(_test_caller_cancellation_reaps_proxy_tasks())


async def _test_caller_cancellation_reaps_proxy_tasks() -> None:
    started = [asyncio.Event(), asyncio.Event()]
    cancelled = [asyncio.Event(), asyncio.Event()]

    async def _blocked(index: int) -> None:
        started[index].set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled[index].set()
            raise

    proxy = asyncio.create_task(
        websocket_proxy._run_until_proxy_closed(asyncio.Event(),
                                                (_blocked(0), _blocked(1))))
    await asyncio.gather(*(event.wait() for event in started))

    proxy.cancel()
    try:
        await proxy
    except asyncio.CancelledError:
        pass

    assert all(event.is_set() for event in cancelled)


def test_stream_error_before_close_signal_reaps_other_tasks() -> None:
    asyncio.run(_test_stream_error_before_close_signal_reaps_other_tasks())


async def _test_stream_error_before_close_signal_reaps_other_tasks() -> None:
    other_cancelled = asyncio.Event()

    async def _fail() -> None:
        raise RuntimeError('stream failed')

    async def _blocked() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            other_cancelled.set()
            raise

    await asyncio.wait_for(
        websocket_proxy._run_until_proxy_closed(asyncio.Event(),
                                                (_fail(), _blocked())), 1)

    assert other_cancelled.is_set()


def test_second_cancellation_waits_for_task_cleanup() -> None:
    asyncio.run(_test_second_cancellation_waits_for_task_cleanup())


async def _test_second_cancellation_waits_for_task_cleanup() -> None:
    cleanup_started = [asyncio.Event(), asyncio.Event()]
    cleanup_release = asyncio.Event()
    cleanup_finished = [asyncio.Event(), asyncio.Event()]

    async def _blocked(index: int) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started[index].set()
            await cleanup_release.wait()
            cleanup_finished[index].set()

    proxy = asyncio.create_task(
        websocket_proxy._run_until_proxy_closed(asyncio.Event(),
                                                (_blocked(0), _blocked(1))))
    await asyncio.sleep(0)
    proxy.cancel()
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in cleanup_started)), 1)
    proxy.cancel()
    cleanup_release.set()

    try:
        await asyncio.wait_for(proxy, 1)
    except asyncio.CancelledError:
        pass

    assert all(event.is_set() for event in cleanup_finished)
