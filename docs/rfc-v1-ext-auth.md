# ArtifactRepository V1 Extension: OAuth 2.0 Bearer Token Authentication

**Status:** Draft\
**Version:** 1.0.0\
**Extends:** ArtifactRepository V1 HTTP API Specification v1.0.0\
**Extension Identifier:** `oauth2-bearer`

## 1. Introduction

This document defines an optional extension to the ArtifactRepository V1 specification that adds authentication and authorization using OAuth 2.0 Bearer Tokens ([RFC 6750](https://datatracker.ietf.org/doc/html/rfc6750)) with OpenID Connect (OIDC) discovery. It is designed primarily for machine-to-machine (M2M) access via the Client Credentials Grant ([RFC 6749 §4.4](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4)), while also supporting interactive user authentication via the Authorization Code Grant.

### 1.1 Motivation

The base V1 specification defines no authentication — all endpoints are open. Production deployments require access control to:

- Restrict who can upload artifacts (write access)
- Restrict who can download artifacts (read access)
- Audit access for compliance
- Integrate with existing organizational identity providers (Entra ID, Okta, Keycloak, Auth0, etc.)

### 1.2 Design Principles

1. **Delegate identity to external providers.** The ArtifactRepository server does NOT manage users, passwords, or credentials. It validates tokens issued by a trusted external OAuth 2.0 Authorization Server / OIDC Provider.
2. **Standard protocols only.** All flows follow OAuth 2.0 ([RFC 6749](https://datatracker.ietf.org/doc/html/rfc6749)), Bearer Token Usage ([RFC 6750](https://datatracker.ietf.org/doc/html/rfc6750)), and OIDC Discovery ([OpenID Connect Discovery 1.0](https://openid.net/specs/openid-connect-discovery-1_0.html)).
3. **Zero client configuration for discovery.** Clients obtain all authentication parameters from the `GET /v1/info` endpoint.
4. **Security by default.** Follow [RFC 9700](https://datatracker.ietf.org/doc/html/rfc9700) (OAuth 2.0 Security Best Current Practice, 2025).

### 1.3 Conventions

The key words "MUST", "MUST NOT", "SHOULD", "SHOULD NOT", and "MAY" are interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

## 2. Capability Advertisement

### 2.1 Server Info Response

Servers implementing this extension MUST advertise it in the `GET /v1/info` response. The `authentication` object MUST include `"oauth2-bearer"` in the `methods` array and MUST include an `oauth2` object with provider details.

```json
{
  "spec_version": "1.0.0",
  "server": {
    "name": "ArtifactRepository",
    "version": "0.1.0"
  },
  "extensions": ["oauth2-bearer"],
  "authentication": {
    "methods": ["oauth2-bearer"],
    "oauth2": {
      "issuer": "https://auth.example.com/",
      "audience": "https://artifacts.example.com/",
      "client_id": "artifact-repo-public-client",
      "scopes": {
        "read": "artifact:read",
        "write": "artifact:write"
      }
    }
  },
  "capabilities": {
    "compression": ["none", "gzip", "deflate", "lzma2", "zstd"],
    "max_object_size": null
  }
}
```

### 2.2 OAuth2 Object Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `issuer` | string (URL) | REQUIRED | The OIDC issuer URL. Clients fetch `{issuer}/.well-known/openid-configuration` for discovery. |
| `audience` | string | REQUIRED | The expected `aud` claim in access tokens. Typically the server's base URL or a logical identifier. |
| `client_id` | string | OPTIONAL | A pre-registered public OAuth 2.0 client ID for interactive flows (Authorization Code + PKCE, Device Authorization). See Section 2.3. |
| `scopes.read` | string | REQUIRED | The OAuth 2.0 scope required for read operations. |
| `scopes.write` | string | REQUIRED | The OAuth 2.0 scope required for write operations. |

The `issuer` URL MUST NOT have a trailing path segment beyond what is necessary for the OIDC discovery document. Clients MUST be able to construct the discovery URL as `{issuer}/.well-known/openid-configuration`.

### 2.3 Client Identity and Credential Provisioning

OAuth 2.0 requires clients to identify themselves to the identity provider. How a client obtains its credentials depends on the grant type:

**Interactive flows (Authorization Code + PKCE, Device Authorization):**

The server SHOULD advertise a `client_id` in the oauth2 object. This is a **public client** ([RFC 6749 §2.1](https://datatracker.ietf.org/doc/html/rfc6749#section-2.1)) pre-registered with the identity provider by the server administrator. Public clients do not have a client secret — they rely on PKCE or device codes for security. This allows any user to authenticate interactively without needing credentials provisioned in advance.

If no `client_id` is advertised, the client MUST obtain its own client registration from the identity provider through out-of-band means.

**Machine-to-machine flows (Client Credentials Grant):**

Client credentials (`client_id` + `client_secret` or private key) MUST be provisioned out-of-band. The server CANNOT advertise these because:

1. Each M2M integration (CI pipeline, build agent, service account) SHOULD have its own unique client registration for auditing and revocation.
2. Client secrets are confidential and cannot be exposed via a public endpoint.

The typical provisioning workflow is:

1. An administrator registers a client application in the identity provider (Entra ID, Keycloak, Okta, etc.).
2. The administrator grants the client the appropriate scopes (`artifact:read`, `artifact:write`).
3. The administrator provides the `client_id` and `client_secret` to the service that needs access (e.g., as CI/CD pipeline secrets).
4. The service discovers all other parameters (issuer, audience, token endpoint, scopes) from `GET /v1/info`.

**Summary of what is discovered vs. configured:**

| Parameter | Interactive (Auth Code) | M2M (Client Credentials) |
|-----------|------------------------|--------------------------|
| Issuer | Discovered from `/v1/info` | Discovered from `/v1/info` |
| Audience | Discovered from `/v1/info` | Discovered from `/v1/info` |
| Scopes | Discovered from `/v1/info` | Discovered from `/v1/info` |
| Token endpoint | Discovered from OIDC discovery | Discovered from OIDC discovery |
| Client ID | Discovered from `/v1/info` | **Configured out-of-band** |
| Client Secret | Not needed (public client + PKCE) | **Configured out-of-band** |

## 3. Token Format

### 3.1 Access Token Requirements

Access tokens MUST be JSON Web Tokens (JWTs) as defined in [RFC 7519](https://datatracker.ietf.org/doc/html/rfc7519), signed using an asymmetric algorithm (RS256, RS384, RS512, ES256, ES384, or ES512).

Servers MUST NOT accept:
- Unsigned tokens (algorithm `"none"`)
- Symmetric-key signed tokens (HS256, HS384, HS512)
- Opaque (non-JWT) tokens

### 3.2 Required Claims

The server MUST validate the following JWT claims:

| Claim | Validation Rule |
|-------|----------------|
| `iss` (Issuer) | MUST exactly match the `issuer` from the server configuration |
| `aud` (Audience) | MUST contain the `audience` value from the server configuration. The `aud` claim MAY be a string or an array; if an array, at least one element MUST match. |
| `exp` (Expiration) | MUST be in the future. Servers SHOULD allow a clock skew tolerance of no more than 60 seconds. |
| `iat` (Issued At) | MUST be present and MUST NOT be in the future (with clock skew tolerance). |
| `scope` or `scp` | MUST contain the required scope for the requested operation (see Section 4). The claim MAY be a space-delimited string ([RFC 6749 §3.3](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3)) or an array of strings. Servers MUST accept both formats. |

### 3.3 Optional Claims

Servers MAY use the following claims for auditing and logging:

| Claim | Description |
|-------|-------------|
| `sub` (Subject) | The principal (user or service) the token represents |
| `azp` (Authorized Party) | The client ID of the application that requested the token |
| `nbf` (Not Before) | If present, the token MUST NOT be accepted before this time |
| `jti` (JWT ID) | Unique token identifier, useful for revocation and audit trails |

### 3.4 Signature Verification

Servers MUST verify JWT signatures using the public keys published at the OIDC provider's `jwks_uri` (obtained from the discovery document). Servers MUST:

1. Fetch the JWKS (JSON Web Key Set) from the `jwks_uri`.
2. Cache the JWKS with a reasonable TTL (RECOMMENDED: 1 hour, or honor the `Cache-Control` header from the JWKS response).
3. Match the JWT's `kid` (Key ID) header to a key in the JWKS.
4. If no matching `kid` is found, refresh the JWKS cache once and retry (handles key rotation).
5. Reject the token if no matching key is found after refresh.

## 4. Scopes and Authorization

### 4.1 Scope Definitions

This extension defines two authorization scopes. The actual scope strings are configurable per-server and advertised in `GET /v1/info`.

| Logical Scope | Default String | Grants |
|---------------|----------------|--------|
| Read | `artifact:read` | Download objects, archives, metadata; check missing objects; query server info |
| Write | `artifact:write` | Upload objects and archives. Implicitly includes read access. |

### 4.2 Endpoint Authorization Matrix

| Endpoint | Method | Required Scope |
|----------|--------|---------------|
| `GET /v1/info` | GET | **None** (always public) |
| `GET /v1/object/{hash}` | GET | Read |
| `PUT /v1/object/{hash}` | PUT | Write |
| `POST /v1/object/missing` | POST | Read |
| `GET /v1/archive/{hash}` | GET | Read |
| `POST /v1/archive/{hash}/supplemental` | POST | Read |
| `POST /v1/archive/upload` | POST | Write |
| `GET /v1/index/{hash}/metadata` | GET | Read |

**Note:** `GET /v1/info` MUST remain unauthenticated. This endpoint is the entry point for capability discovery and is required before a client can determine how to authenticate.

### 4.3 Write Implies Read

A token with the write scope MUST be accepted for read operations. Servers MUST NOT require both scopes simultaneously — a write-scoped token is sufficient for all operations.

## 5. Client Authentication Flow

### 5.1 Discovery

Clients MUST perform the following steps before making authenticated requests:

1. Call `GET /v1/info` on the target server.
2. Check `authentication.methods` for `"oauth2-bearer"`.
3. If present, read the `authentication.oauth2` object.
4. Fetch the OIDC discovery document from `{issuer}/.well-known/openid-configuration`.
5. Extract the `token_endpoint` from the discovery document.

### 5.2 Token Acquisition — Client Credentials Grant

For M2M access (CI/CD pipelines, build systems, automation), clients use the OAuth 2.0 Client Credentials Grant:

```http
POST /oauth2/token HTTP/1.1
Host: auth.example.com
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials
&client_id=YOUR_CLIENT_ID
&client_secret=YOUR_CLIENT_SECRET
&scope=artifact:write
&audience=https://artifacts.example.com/
```

The client MUST authenticate to the token endpoint using one of the methods advertised in the discovery document's `token_endpoint_auth_methods_supported`. Common methods:

| Method | Description |
|--------|-------------|
| `client_secret_basic` | HTTP Basic auth with `client_id:client_secret` in the `Authorization` header |
| `client_secret_post` | `client_id` and `client_secret` as POST body parameters |
| `private_key_jwt` | Client authenticates with a signed JWT assertion (RECOMMENDED for high-security environments) |

### 5.3 Token Acquisition — Authorization Code Grant

For interactive user access (developer workstations, browsers), clients MAY use the Authorization Code Grant with PKCE ([RFC 7636](https://datatracker.ietf.org/doc/html/rfc7636)):

1. Open the authorization URL in the user's browser.
2. User authenticates with the identity provider.
3. Client receives an authorization code via redirect.
4. Client exchanges the code for an access token at the token endpoint.

The details of this flow are defined by OAuth 2.0 and PKCE and are not further specified here. Clients implementing this flow MUST use PKCE with the S256 challenge method.

### 5.4 Token Acquisition — Device Authorization Grant

For CLI tools running in environments without a browser (headless servers, SSH sessions), clients MAY use the Device Authorization Grant ([RFC 8628](https://datatracker.ietf.org/doc/html/rfc8628)):

1. Client requests a device code from the identity provider.
2. User visits a URL and enters the device code to authenticate.
3. Client polls the token endpoint until the user completes authentication.

### 5.5 Sending Authenticated Requests

Once a token is obtained, clients MUST include it in the `Authorization` header of every request (except `GET /v1/info`):

```http
GET /v1/object/aabb...128chars... HTTP/1.1
Host: artifacts.example.com
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

Tokens MUST be sent using the `Bearer` scheme as defined in [RFC 6750 §2.1](https://datatracker.ietf.org/doc/html/rfc6750#section-2.1). Servers MUST NOT accept tokens via query parameters or form-encoded body.

### 5.6 Token Refresh

Access tokens obtained via the Client Credentials Grant are typically short-lived. Clients SHOULD:

1. Cache the access token until near its expiration (`exp` claim or `expires_in` response field).
2. Request a new token before the current one expires.
3. NOT use refresh tokens with the Client Credentials Grant (request a new access token directly instead, per [RFC 6749 §4.4.3](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.3)).

Clients SHOULD proactively refresh tokens when 80% of the token's lifetime has elapsed to avoid request failures.

## 6. Server Behavior

### 6.1 Request Validation

For every request (except `GET /v1/info`), the server MUST:

1. Check for the `Authorization` header.
2. Extract the Bearer token.
3. Decode and validate the JWT (Section 3).
4. Verify the token has the required scope for the operation (Section 4).
5. If any step fails, respond with the appropriate error (Section 7).

### 6.2 OIDC Configuration Caching

Servers MUST cache the OIDC discovery document and JWKS to avoid per-request HTTP calls to the identity provider. Recommended cache behavior:

| Resource | Cache TTL |
|----------|-----------|
| Discovery document | 24 hours or per `Cache-Control` |
| JWKS | 1 hour or per `Cache-Control`, with on-demand refresh on `kid` miss |

### 6.3 Multiple Issuers

A server MAY support multiple OIDC issuers simultaneously. When multiple issuers are configured:

- The `GET /v1/info` response SHOULD include all issuers.
- The server determines which issuer to validate against by inspecting the `iss` claim in the JWT.
- Each issuer has its own audience, JWKS, and configuration.

When a server supports multiple identity providers, the `oauth2` field MAY be an array:

```json
{
  "authentication": {
    "methods": ["oauth2-bearer"],
    "oauth2": [
      {
        "issuer": "https://auth.example.com/",
        "audience": "https://artifacts.example.com/",
        "client_id": "artifact-repo-public-client",
        "scopes": { "read": "artifact:read", "write": "artifact:write" }
      },
      {
        "issuer": "https://login.microsoftonline.com/{tenant}/v2.0",
        "audience": "api://artifact-repository",
        "client_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "scopes": { "read": "Artifacts.Read", "write": "Artifacts.ReadWrite" }
      }
    ]
  }
}
```

Clients SHOULD use the first issuer in the array unless they have a preference for a specific provider. When `oauth2` is an object (not an array), it is equivalent to a single-element array.

## 7. Error Responses

### 7.1 HTTP Status Codes

| Status | When | Description |
|--------|------|-------------|
| 401 Unauthorized | No token provided, or token is invalid (expired, bad signature, wrong issuer/audience) | Client must obtain a valid token |
| 403 Forbidden | Token is valid but lacks the required scope | Client must obtain a token with appropriate scopes |

### 7.2 WWW-Authenticate Header

On 401 responses, the server MUST include a `WWW-Authenticate` header per [RFC 6750 §3](https://datatracker.ietf.org/doc/html/rfc6750#section-3):

**No token provided:**
```http
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer realm="ArtifactRepository"
```

**Invalid token:**
```http
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer realm="ArtifactRepository", error="invalid_token", error_description="Token has expired"
```

**Insufficient scope:**
```http
HTTP/1.1 403 Forbidden
WWW-Authenticate: Bearer realm="ArtifactRepository", error="insufficient_scope", scope="artifact:write"
```

### 7.3 Error Response Body

Error responses SHOULD include a JSON body for programmatic consumption:

```json
{
  "error": "invalid_token",
  "error_description": "Token has expired"
}
```

| Error Code | HTTP Status | Description |
|------------|-------------|-------------|
| `missing_token` | 401 | No `Authorization` header or Bearer token found |
| `invalid_token` | 401 | Token is malformed, has an invalid signature, is expired, or has wrong issuer/audience |
| `insufficient_scope` | 403 | Token is valid but does not contain the required scope |

## 8. Client Configuration

### 8.1 Required Configuration

**Interactive users (Authorization Code + PKCE, Device Authorization):**

| Parameter | Source | Description |
|-----------|--------|-------------|
| Server URL | User-provided | The ArtifactRepository server base URL |

All other parameters (issuer, audience, scopes, client_id, token endpoint) are discovered automatically from `GET /v1/info` and the OIDC discovery document. No credentials need to be configured — the user authenticates interactively via their browser or device code.

**Machine-to-machine (Client Credentials Grant):**

| Parameter | Source | Description |
|-----------|--------|-------------|
| Server URL | Configured | The ArtifactRepository server base URL |
| Client ID | Configured (out-of-band) | OAuth 2.0 client identifier, registered with the identity provider |
| Client Secret or Key | Configured (out-of-band) | Client authentication credential (secret, key file, or certificate) |

All other parameters (issuer, audience, scopes, token endpoint) are discovered automatically.

### 8.2 Configuration Sources

Clients SHOULD support configuration via (in precedence order):

1. Command-line arguments (e.g., `--client-id`, `--client-secret`)
2. Environment variables (e.g., `ARTIFACT_CLIENT_ID`, `ARTIFACT_CLIENT_SECRET`)
3. Configuration file (e.g., `~/.config/artifact-repository/auth.json`)

Client secrets MUST NOT be logged, displayed, or included in error messages.

## 9. Transport Security

### 9.1 TLS Requirement

When this extension is active, all communication between client and server MUST use TLS 1.2 or later (HTTPS). Servers MUST reject non-TLS connections with a redirect or error.

The only exception is local development: servers MAY allow non-TLS connections to `localhost` or `127.0.0.1` addresses.

### 9.2 Token Transmission

Tokens MUST only be transmitted over TLS-protected connections. Clients MUST NOT send Bearer tokens over plaintext HTTP (except to localhost for development).

## 10. Compatibility

### 10.1 Unauthenticated Servers

Servers that do not implement this extension return an empty `methods` array in `GET /v1/info`:

```json
{
  "authentication": {
    "methods": []
  }
}
```

Clients MUST treat an empty `methods` array as "no authentication required" and proceed without tokens.

### 10.2 Mixed Access

Servers MAY support both authenticated and unauthenticated access simultaneously (e.g., public read, authenticated write). In this case, the server:

- Accepts requests without tokens for read operations
- Requires tokens only for write operations
- Advertises `"oauth2-bearer"` in `methods` to indicate that authentication is available

The server's authorization policy determines whether unauthenticated read access is permitted. This is a deployment decision, not a protocol concern.

### 10.3 Unsupporting Clients

Clients that do not support this extension and encounter `"oauth2-bearer"` in the `methods` array:

- SHOULD inform the user that the server requires authentication they cannot provide
- MUST NOT attempt to access protected endpoints without a token

## 11. Security Considerations

### 11.1 Token Storage

Clients SHOULD store tokens in memory only. If persistent storage is necessary:

- Use OS-level credential storage (e.g., OS keychain, credential manager)
- Set restrictive file permissions (mode 0600)
- Never store tokens in version-controlled files

### 11.2 Token Lifetime

Access tokens SHOULD have a lifetime of no more than 1 hour. Shorter lifetimes (5–15 minutes) are RECOMMENDED for high-security environments.

### 11.3 Scope Minimization

Clients SHOULD request the minimum scope necessary for their operation. A read-only client SHOULD NOT request write scope.

### 11.4 Algorithm Restrictions

Per [RFC 9700](https://datatracker.ietf.org/doc/html/rfc9700), servers MUST:

- Reject tokens with `"alg": "none"`
- Reject tokens signed with symmetric algorithms (HS256, etc.) unless the server is also the issuer
- Prefer ES256 or RS256 for signature verification

### 11.5 Audience Validation

Audience validation prevents tokens issued for other services from being accepted. Servers MUST reject tokens whose `aud` claim does not match their configured audience, even if the token is otherwise valid and issued by a trusted issuer.

## 12. References

| Reference | Title |
|-----------|-------|
| [RFC 6749](https://datatracker.ietf.org/doc/html/rfc6749) | The OAuth 2.0 Authorization Framework |
| [RFC 6750](https://datatracker.ietf.org/doc/html/rfc6750) | The OAuth 2.0 Authorization Framework: Bearer Token Usage |
| [RFC 7519](https://datatracker.ietf.org/doc/html/rfc7519) | JSON Web Token (JWT) |
| [RFC 7636](https://datatracker.ietf.org/doc/html/rfc7636) | Proof Key for Code Exchange (PKCE) |
| [RFC 8628](https://datatracker.ietf.org/doc/html/rfc8628) | OAuth 2.0 Device Authorization Grant |
| [RFC 9700](https://datatracker.ietf.org/doc/html/rfc9700) | OAuth 2.0 Security Best Current Practice |
| [OIDC Discovery](https://openid.net/specs/openid-connect-discovery-1_0.html) | OpenID Connect Discovery 1.0 |
