"""The host module broker's organization-scoped workload credential exchange.

Credential callbacks read the composing runtime's own credentials. No user
credential, authorization decision, retry or cross-organization cache lives here.
"""

import asyncio
import ipaddress
import json
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from .authorization_revision import RevisionUnavailable, _decode, _internal_credential


def _origin(value, mesh_protected):
    try:
        url = urlsplit(value)
        host = url.hostname or ""
        port = url.port
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = host == "localhost"
        mesh = mesh_protected and host.endswith((".svc", ".svc.cluster.local"))
        if (
            not host
            or url.username is not None
            or url.password is not None
            or url.path not in ("", "/")
            or url.query
            or url.fragment
            or port == 0
            or "\\" in value
            or any(c.isspace() for c in value)
            or url.scheme not in ("http", "https")
            or (url.scheme == "http" and not (loopback or mesh))
        ):
            raise ValueError
    except ValueError:
        raise ValueError(
            "Module broker requires HTTPS or an explicitly protected mesh origin"
        ) from None
    return value.rstrip("/")


class ModuleClient:
    """Mint this workload's identity for one organization, without caching it."""

    def __init__(
        self,
        origin,
        prefix,
        internal_credential,
        workload_credential,
        *,
        mesh_protected=False,
        transport=None,
    ):
        self.endpoint = _origin(origin, mesh_protected) + "/modules/_work-context"
        if not prefix or len(prefix) > 128 or any(c.isspace() for c in prefix):
            raise ValueError("A module prefix is required")
        self.prefix = prefix
        self.internal_credential = internal_credential
        self.workload_credential = workload_credential
        self.client = httpx.AsyncClient(
            transport=transport, trust_env=False, follow_redirects=False
        )

    async def work_context(self, organization):
        try:
            return await asyncio.wait_for(self._mint(organization), timeout=3)
        except (
            httpx.HTTPError,
            OSError,
            ValueError,
            TypeError,
            AttributeError,
            TimeoutError,
            RevisionUnavailable,
        ):
            # Broker bodies, transport details and credentials never escape.
            raise RevisionUnavailable("Module identity is unavailable") from None

    async def _mint(self, organization):
        if str(UUID(organization)) != organization:
            raise ValueError("Organization must be a canonical UUID")
        internal = await _internal_credential(self.internal_credential)
        workload = await _internal_credential(self.workload_credential)
        request = httpx.Request(
            "POST",
            self.endpoint,
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "accept-encoding": "identity",
                "x-codefly-internal-token": internal,
                "authorization": "Bearer " + workload,
            },
            content=json.dumps(
                {"prefix": self.prefix, "organization_id": organization}
            ).encode(),
        )
        response = await self.client.send(request, stream=True, auth=None)
        try:
            if (
                response.status_code != 200
                or response.headers.get("content-type", "").split(";", 1)[0]
                != "application/json"
                or response.headers.get("content-encoding", "identity") != "identity"
            ):
                raise ValueError("Module broker refused the exchange")
            data = bytearray()
            async for chunk in response.aiter_bytes():
                data.extend(chunk)
                if len(data) > 65536:
                    raise ValueError("Module broker response exceeds bound")
            payload = _decode(data)
            if payload.get("tenant") != organization or not payload.get("principalId"):
                raise ValueError("Module broker returned a different identity")

            async def minted():
                return payload.get("token")

            return await _internal_credential(minted)
        finally:
            await response.aclose()

    async def aclose(self):
        await self.client.aclose()
