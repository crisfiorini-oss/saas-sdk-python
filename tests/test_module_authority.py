import json

import httpx
import pytest

from saas_sdk.authorization_revision import RevisionUnavailable
from saas_sdk.module_authority import ModuleClient

pytestmark = pytest.mark.asyncio
ORG = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"


async def internal():
    return "internal-fixture"


async def workload():
    return "workload-fixture"


def client(respond):
    return ModuleClient(
        "https://broker.invalid",
        "example",
        internal,
        workload,
        transport=httpx.MockTransport(respond),
    )


async def test_organization_and_rotating_own_credential_on_every_exchange():
    seen = []

    async def respond(request):
        body = json.loads(request.content)
        seen.append((body, dict(request.headers)))
        assert request.url.path == "/modules/_work-context"
        return httpx.Response(
            200,
            json={
                "token": "module-" + body["organization_id"],
                "tenant": body["organization_id"],
                "principalId": "example",
            },
        )

    c = client(respond)
    try:
        assert await c.work_context(ORG) == "module-" + ORG

        async def rotated():
            return "rotated-fixture"

        c.workload_credential = rotated
        assert await c.work_context(OTHER) == "module-" + OTHER
        assert [b for b, _ in seen] == [
            {"prefix": "example", "organization_id": ORG},
            {"prefix": "example", "organization_id": OTHER},
        ]
        assert [h["authorization"] for _, h in seen] == [
            "Bearer workload-fixture",
            "Bearer rotated-fixture",
        ]
        assert all(h["x-codefly-internal-token"] == "internal-fixture" for _, h in seen)
        assert all(
            "x-codefly-work-context" not in h and "cookie" not in h for _, h in seen
        )
    finally:
        await c.aclose()


@pytest.mark.parametrize(
    "status,payload",
    [
        (401, {"error": "private-secret"}),
        (403, {}),
        (302, {}),
        (503, {}),
        (200, {"token": "private-secret", "tenant": OTHER, "principalId": "example"}),
        (200, {"token": "bad\ncredential", "tenant": ORG, "principalId": "example"}),
        (200, {"token": "x" * 9000, "tenant": ORG, "principalId": "example"}),
    ],
)
async def test_refusal_is_redacted_and_not_retried(status, payload):
    calls = []

    async def respond(request):
        calls.append(request)
        return httpx.Response(status, json=payload)

    c = client(respond)
    try:
        with pytest.raises(
            RevisionUnavailable, match="^Module identity is unavailable$"
        ):
            await c.work_context(ORG)
        assert len(calls) == 1
    finally:
        await c.aclose()


async def test_invalid_organization_never_sends_credentials():
    def respond(request):
        pytest.fail("invalid selector reached broker")

    c = client(respond)
    try:
        with pytest.raises(RevisionUnavailable):
            await c.work_context("not-an-organization")
    finally:
        await c.aclose()


@pytest.mark.parametrize(
    "origin",
    [
        "http://public.invalid",
        "https://user:secret@broker.invalid",
        "https://broker.invalid/other",
    ],
)
async def test_refuse_unsafe_broker(origin):
    with pytest.raises(ValueError, match="Module broker requires"):
        ModuleClient(origin, "example", internal, workload, mesh_protected=True)
