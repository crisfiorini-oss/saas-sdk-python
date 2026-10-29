"""Owner protocol conformance, without consumer permissions or provider calls."""

import asyncio
import json
import ssl
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import grpc
import httpx
import pytest
import pytest_asyncio
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from google.protobuf.json_format import ParseDict

from saas_sdk import authorization_revision as revision
from saas_sdk import work_context as wc
from saas_sdk._gen import work_contexts_pb2 as pb

pytestmark = pytest.mark.asyncio


def projection_fixture():
    return json.loads(
        (
            Path(__file__).parent / "fixtures/authorization_revision_projection.json"
        ).read_text()
    )


def claims():
    value = projection_fixture()["claims"]

    def scopes(values):
        return tuple(
            wc.WorkScope(
                s["resource_kind"],
                tuple(s["actions"]),
                tuple(s.get("resource_ids", ())),
            )
            for s in values
        )

    return wc.WorkContext(
        typ=wc.WORK_CONTEXT_TYPE,
        algorithm="Ed25519",
        key_id="key",
        issuer="issuer",
        audience="audience",
        not_before_unix=1,
        issued_at_unix=1,
        expires_at_unix=2,
        nonce="nonce",
        authorization_revision=int(value["authorization_revision"]),
        replay_policy="idempotent",
        tenant_id=value["tenant_id"],
        owner_principal_id=value["owner_principal_id"],
        task_id="task",
        session_id="session",
        parent_session_id=None,
        authority_scopes=scopes(value["authority_scopes"]),
        actor_chain=tuple(
            wc.WorkActor(
                a["principal_id"], "service", "delegation", scopes(a["granted_scopes"])
            )
            for a in value["actor_chain"]
        ),
        attribution_team_ids=(),
        workspace_id=None,
        project_id=None,
    )


async def token():
    return "internal-fixture"


async def test_shared_projection():
    value = claims()
    request = revision.revision_request(value)
    assert request == ParseDict(
        projection_fixture()["request"], pb.CheckAuthorizationRevisionRequest()
    )
    request.subjects[0].scopes[0].actions[0] = "mutated"
    assert value.authority_scopes[0].actions == ("read", "write")
    assert (
        revision.REVISION_PATH
        == "/saas.accounts.v1.WorkContextService/CheckAuthorizationRevision"
    )


@pytest.mark.parametrize(
    "status,body,error",
    [
        (200, b"{}", None),
        (200, b"null", revision.RevisionUnavailable),
        (200, b"[]", revision.RevisionUnavailable),
        (200, b"true", revision.RevisionUnavailable),
        (200, b"{}{}", revision.RevisionUnavailable),
        (200, b'{"x":1}', revision.RevisionUnavailable),
        (200, b'{"x":1,"x":2}', revision.RevisionUnavailable),
        (200, b'{"x":NaN}', revision.RevisionUnavailable),
        (200, b" " * 8193, revision.RevisionUnavailable),
        (400, b'{"code":"failed_precondition"}', revision.RevisionDenied),
        (
            403,
            b'{"code":"permission_denied","message":"private-detail"}',
            revision.RevisionDenied,
        ),
        (400, b'{"code":"permission_denied"}', revision.RevisionUnavailable),
        (403, b'{"code":"failed_precondition"}', revision.RevisionUnavailable),
        (401, b'{"code":"permission_denied"}', revision.RevisionUnavailable),
        (307, b'{"code":"permission_denied"}', revision.RevisionUnavailable),
        (503, b'{"code":"permission_denied"}', revision.RevisionUnavailable),
        (
            403,
            b'{"code":"permission_denied","code":"permission_denied"}',
            revision.RevisionUnavailable,
        ),
    ],
)
async def test_connect_exact_empty_and_coherent_errors(status, body, error):
    calls = []

    async def respond(request):
        calls.append(request)
        return httpx.Response(
            status,
            content=body,
            headers={
                "content-type": "application/json",
                "location": "https://redirect.invalid",
            },
        )

    client = revision.ConnectClient(
        "https://accounts.invalid", token, transport=httpx.MockTransport(respond)
    )
    try:
        if error is None:
            await client.check(claims())
        else:
            with pytest.raises(error) as caught:
                await client.check(claims())
            assert "private-detail" not in str(caught.value)
        assert len(calls) == 1
    finally:
        await client.aclose()


async def test_connect_refresh_revocation_and_no_ambient_credentials():
    current = [42, "internal-one"]
    calls = []
    credential_calls = []

    async def credential():
        credential_calls.append(current[1])
        return current[1]

    async def respond(request):
        calls.append(request)
        assert request.headers["x-codefly-internal-token"] == current[1]
        assert not {"cookie", "authorization", "x-codefly-work-context"}.intersection(
            request.headers
        )
        body = json.loads(request.content)
        if int(body["authorization_revision"]) != current[0]:
            return httpx.Response(400, json={"code": "failed_precondition"})
        return httpx.Response(200, json={})

    client = revision.ConnectClient(
        "https://accounts.invalid", credential, transport=httpx.MockTransport(respond)
    )
    client.client.cookies.set("private", "caller")
    client.client.headers["authorization"] = "Bearer caller"
    try:
        await client.check(claims())
        current[:] = [43, "internal-two"]
        with pytest.raises(revision.RevisionDenied):
            await client.check(claims())
        await client.check(replace(claims(), authorization_revision=43))
        assert len(calls) == len(credential_calls) == 3
    finally:
        await client.aclose()


@pytest.mark.parametrize("value", [None, "", "with space", "bad\nvalue", "x" * 8193])
async def test_invalid_credential_does_not_dispatch(value):
    calls = []

    async def credential():
        return value

    async def respond(request):
        calls.append(request)
        return httpx.Response(200, json={})

    client = revision.ConnectClient(
        "https://accounts.invalid", credential, transport=httpx.MockTransport(respond)
    )
    try:
        with pytest.raises(revision.RevisionUnavailable):
            await client.check(claims())
        assert calls == []
    finally:
        await client.aclose()


async def test_connect_total_budget_includes_credential_and_stream(monkeypatch):
    monkeypatch.setattr(revision, "TIMEOUT", 0.08)
    cancelled = asyncio.Event()
    closed = asyncio.Event()

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            try:
                await asyncio.sleep(10)
                yield b"{}"
            finally:
                cancelled.set()

        async def aclose(self):
            closed.set()

    async def credential():
        await asyncio.sleep(0.03)
        return "internal-fixture"

    async def respond(request):
        return httpx.Response(
            200, stream=Body(), headers={"content-type": "application/json"}
        )

    client = revision.ConnectClient(
        "https://accounts.invalid", credential, transport=httpx.MockTransport(respond)
    )
    try:
        with pytest.raises(revision.RevisionUnavailable):
            await client.check(claims())
        assert cancelled.is_set() and closed.is_set()
    finally:
        await client.aclose()


@pytest_asyncio.fixture(params=["tls", "mesh"])
async def grpc_endpoint(request, tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(key, hashes.SHA256())
    )
    cert_bytes = cert.public_bytes(serialization.Encoding.PEM)
    key_bytes = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    state = {
        "calls": [],
        "status": None,
        "response": b"",
        "delay": 0,
        "cancelled": asyncio.Event(),
        "started": asyncio.Event(),
    }

    async def check(request, context):
        state["calls"].append((request, dict(context.invocation_metadata())))
        state["started"].set()
        try:
            if state["delay"]:
                await asyncio.sleep(state["delay"])
            if state["status"]:
                await context.abort(state["status"], "private-detail")
            return state["response"]
        except asyncio.CancelledError:
            state["cancelled"].set()
            raise

    server = grpc.aio.server()
    handler = grpc.unary_unary_rpc_method_handler(
        check,
        request_deserializer=pb.CheckAuthorizationRevisionRequest.FromString,
        response_serializer=lambda value: value,
    )
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "saas.accounts.v1.WorkContextService",
                {"CheckAuthorizationRevision": handler},
            ),
        )
    )
    if request.param == "tls":
        port = server.add_secure_port(
            "localhost:0", grpc.ssl_server_credentials(((key_bytes, cert_bytes),))
        )
    else:
        # The current host's transport: h2c, with the mesh securing the hop.
        port = server.add_insecure_port("localhost:0")
    await server.start()
    state["address"] = f"localhost:{port}"
    if request.param == "tls":
        tls = ssl.create_default_context(cadata=cert_bytes.decode())
        client = revision.GRPCClient(f"https://localhost:{port}", token, tls_context=tls)
    else:
        client = revision.MeshGRPCClient(f"localhost:{port}", token)
    try:
        yield client, state
    finally:
        await client.aclose()
        await server.stop(None)


async def test_grpc_canonical_wire_credential_refresh_and_current_denial(grpc_endpoint):
    client, state = grpc_endpoint
    await client.check(claims())

    async def rotated():
        return "internal-rotated"

    client.credential = rotated
    state["status"] = grpc.StatusCode.FAILED_PRECONDITION
    with pytest.raises(revision.RevisionDenied):
        await client.check(claims())
    assert len(state["calls"]) == 2
    assert [m["x-codefly-internal-token"] for _, m in state["calls"]] == [
        "internal-fixture",
        "internal-rotated",
    ]
    for request, metadata in state["calls"]:
        assert request == revision.revision_request(claims())
        assert not {"authorization", "cookie", "x-codefly-work-context"}.intersection(
            metadata
        )


@pytest.mark.parametrize("grpc_endpoint", ["mesh"], indirect=True)
async def test_module_revision_uses_work_organization_and_separate_module_proof(grpc_endpoint):
    _, state = grpc_endpoint

    class Module:
        def __init__(self):
            self.organizations = []
            self.closed = False

        async def work_context(self, org):
            self.organizations.append(org)
            return "own-module-proof"

        async def aclose(self):
            self.closed = True

    module = Module()
    client = revision.ModuleMeshGRPCClient(state["address"], module)
    try:
        await client.check(claims())
        state["status"] = grpc.StatusCode.FAILED_PRECONDITION
        with pytest.raises(revision.RevisionDenied):
            await client.check(claims())
        assert module.organizations == [claims().tenant_id] * 2
        for request, metadata in state["calls"]:
            assert request == revision.revision_request(claims())
            assert metadata["x-codefly-work-context"] == "own-module-proof"
            assert not {"authorization", "cookie", "x-codefly-internal-token"}.intersection(metadata)
    finally:
        await client.aclose()
    assert module.closed


@pytest.mark.parametrize(
    "status,error",
    [
        (grpc.StatusCode.PERMISSION_DENIED, revision.RevisionDenied),
        (grpc.StatusCode.FAILED_PRECONDITION, revision.RevisionDenied),
        (grpc.StatusCode.UNAUTHENTICATED, revision.RevisionUnavailable),
        (grpc.StatusCode.UNAVAILABLE, revision.RevisionUnavailable),
    ],
)
async def test_grpc_typed_errors_no_retry(grpc_endpoint, status, error):
    client, state = grpc_endpoint
    state["status"] = status
    with pytest.raises(error) as caught:
        await client.check(claims())
    assert "private-detail" not in str(caught.value)
    assert len(state["calls"]) == 1


@pytest.mark.parametrize("body", [b"\x08\x01", b"x" * 8193])
async def test_grpc_exact_empty_response(grpc_endpoint, body):
    client, state = grpc_endpoint
    state["response"] = body
    with pytest.raises(revision.RevisionUnavailable):
        await client.check(claims())
    assert len(state["calls"]) == 1


async def test_grpc_caller_cancellation(grpc_endpoint):
    client, state = grpc_endpoint
    state["delay"] = 10
    task = asyncio.create_task(client.check(claims()))
    await asyncio.wait_for(state["started"].wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(state["cancelled"].wait(), 1)
    assert len(state["calls"]) == 1


async def test_grpc_total_budget_includes_credential(grpc_endpoint, monkeypatch):
    monkeypatch.setattr(revision, "TIMEOUT", 0.1)
    client, state = grpc_endpoint

    async def delayed():
        await asyncio.sleep(0.03)
        return "internal-fixture"

    client.credential = delayed
    state["delay"] = 10
    with pytest.raises(revision.RevisionUnavailable):
        await client.check(claims())
    await asyncio.wait_for(state["cancelled"].wait(), 1)
    assert len(state["calls"]) == 1


@pytest.mark.parametrize(
    "origin",
    [
        "http://accounts.invalid",
        "https://user@accounts.invalid",
        "https://accounts.invalid/path",
        "https://accounts.invalid?other=x",
        "https://accounts.invalid#other",
        "https://accounts.invalid:0",
    ],
)
async def test_explicit_https_origin_required(origin):
    for kind in (revision.GRPCClient, revision.ConnectClient):
        with pytest.raises(ValueError):
            kind(origin, token)


async def test_tls_verification_cannot_be_disabled():
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls.check_hostname = False
    for kind in (revision.GRPCClient, revision.ConnectClient):
        with pytest.raises(ValueError, match="cannot be disabled"):
            kind("https://accounts.invalid", token, tls_context=tls)


async def test_credential_cancellation_prevents_dispatch(monkeypatch):
    monkeypatch.setattr(revision, "TIMEOUT", 0.03)
    cancelled = asyncio.Event()

    async def credential():
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.set()

    async def respond(request):
        pytest.fail("credential failure dispatched")

    client = revision.ConnectClient(
        "https://accounts.invalid", credential, transport=httpx.MockTransport(respond)
    )
    try:
        with pytest.raises(revision.RevisionUnavailable):
            await client.check(claims())
        assert cancelled.is_set()
    finally:
        await client.aclose()


@pytest.mark.parametrize(
    "address",
    [
        "",
        "accounts.invalid",
        "accounts.invalid:0",
        "accounts.invalid:65536",
        "accounts.invalid:port",
        "http://accounts.invalid:8080",
        "https://accounts.invalid:8080",
        "//accounts.invalid:8080",
        "user@accounts.invalid:8080",
        "accounts.invalid:8080/path",
        "accounts.invalid:8080?x=1",
        "accounts.invalid:8080#x",
        "accounts.invalid :8080",
        ":8080",
    ],
)
async def test_mesh_address_is_a_resolved_host_and_port(address):
    with pytest.raises(ValueError, match="host:port"):
        revision.MeshGRPCClient(address, token)


@pytest.mark.parametrize(
    "address,target",
    [
        ("accounts.platform-saas.svc.cluster.local:8080", "accounts.platform-saas.svc.cluster.local:8080"),
        ("127.0.0.1:8080", "127.0.0.1:8080"),
        ("[::1]:8080", "[::1]:8080"),
    ],
)
async def test_mesh_address_accepts_resolved_forms(address, target):
    assert revision._mesh_address(address) == target
    client = revision.MeshGRPCClient(address, token)
    await client.aclose()
