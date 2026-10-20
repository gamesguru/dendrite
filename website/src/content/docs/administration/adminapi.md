---
title: Supported admin APIs
description: Administrative API endpoints for Zendrite
---

Zendrite supports, at present, a very small number of endpoints that allow admin users to perform administrative functions.
Please note that there is no API stability guarantee on these endpoints at present — they may change shape without warning.

More endpoints will be added in the future.

Endpoints may be used directly through curl:

```bash
curl --header "Authorization: Bearer <access_token>" -X <POST|GET|PUT> <Endpoint URI> -d '<Request Body Contents>'
```

An `access_token` can be obtained through most Element-based matrix clients by going to `Settings` -> `Help & About` -> `Advanced` -> `Access Token`.
Be aware that an `access_token` allows a client to perform actions as an user and should be kept **secret**.

The user must be an administrator in the `userapi_accounts` table in order to use these endpoints.

Existing user accounts can be set to administrative accounts by changing `account_type` to `3` in `userapi_accounts`

```sql
UPDATE userapi_accounts SET account_type = 3 WHERE localpart = '$localpart';
```

Where `$localpart` is the username only (e.g. `alice`).

## POST `/_zendrite/admin/evacuateRoom/{roomID}`

This endpoint will instruct Zendrite to part all local users from the given `roomID` in the URL.
It may take some time to complete.
A JSON body will be returned containing the user IDs of all affected users.

If the room has an alias set (e.g. is published), the room's ID will not be visible in the URL, but it can be found as the room's "internal ID" in Element Web (Settings -> Advanced)

## POST `/_zendrite/admin/evacuateUser/{userID}`

This endpoint will instruct Zendrite to part the given local `userID` in the URL from all rooms which they are currently joined.
A JSON body will be returned containing the room IDs of all affected rooms.

## POST `/_zendrite/admin/resetPassword/{userID}`

Reset the password of a local user.

**If `logout_devices` is set to `true`, all `access_tokens` will be invalidated, resulting in the potential loss of encrypted messages**

Request body format:

```json
{
  "password": "new_password_here",
  "logout_devices": false
}
```

## GET `/_zendrite/admin/fulltext/reindex`

This endpoint instructs Zendrite to reindex all searchable events (`m.room.message`, `m.room.topic` and `m.room.name`).
An empty JSON body will be returned immediately.
Indexing is done in the background, the server logs every 1000 events (or below) when they are being indexed.
Once reindexing is done, you'll see something along the lines `Indexed 69586 events in 53.68223182s` in your debug logs.

## POST `/_zendrite/admin/refreshDevices/{userID}`

This endpoint instructs Zendrite to immediately query `/devices/{userID}` on a federated server.
An empty JSON body will be returned on success, updating all locally stored user devices/keys.
This can be used to possibly resolve E2EE issues, where the remote user can't decrypt messages.

## POST `/_zendrite/admin/purgeRoom/{roomID}`

This endpoint instructs Zendrite to remove the given room from its database.
Before doing so, it attempts to evacuate all local users from the room.
It does **NOT** remove media files.
Depending on the size of the room, this may take a while.
Will return an empty JSON once other components were instructed to delete the room.

Zendrite can also purge empty rooms automatically — see [Auto-purging empty rooms](/administration/auto-purge-empty-rooms/).

## GET `/_zendrite/admin/emptyRooms`

Returns a list of all rooms which have zero (locally) joined members.
Response format:

```json
{
  "empty_rooms": ["!roomid1:server_name", "!roomid2:server_name"]
}
```

## POST `/_synapse/admin/v1/send_server_notice`

Request body format:

```json
{
  "user_id": "@target_user:server_name",
  "content": {
    "msgtype": "m.text",
    "body": "This is my message"
  }
}
```

Send a server notice to a specific user.
See the [Matrix Spec](https://spec.matrix.org/v1.3/client-server-api/#server-notices) for additional details on server notice behaviour.
If successfully sent, the API will return the following response:

```json
{
  "event_id": "<event_id>"
}
```

## GET `/_synapse/admin/v1/register`

Shared secret registration — please see the [user creation page](/administration/createusers) for guidance on configuring and using this endpoint.

## GET and PUT `/_matrix/client/v1/admin/lock/{userId}`

From the [Matrix Spec](https://spec.matrix.org/v1.18/client-server-api/#put_matrixclientv1adminlockuserid).
`PUT` locks (`{"locked": true}`) or unlocks (`{"locked": false}`) a local account, and `GET` returns `{"locked": <bool>}`.
A locked account keeps its sessions but can only log out until it is unlocked; see [account locking](/mscs/#account-locking).
The user ID must belong to this server (otherwise 400 `M_INVALID_PARAM`), and the account must exist and not be deactivated (otherwise 404 `M_NOT_FOUND`).
Administrators cannot lock themselves or another administrator (403 `M_FORBIDDEN`).
With MSC3861 delegated authentication, both methods return 403; lock the account in the authentication service instead.

## GET and PUT `/_matrix/client/v1/admin/suspend/{userId}`

From the [Matrix Spec](https://spec.matrix.org/v1.18/client-server-api/#put_matrixclientv1adminsuspenduserid).
`PUT` suspends (`{"suspended": true}`) or unsuspends (`{"suspended": false}`) a local account, and `GET` returns `{"suspended": <bool>}`.
A suspended account can still read and log in, but not join rooms, send messages or change its profile; see [account suspension](/mscs/#account-suspension).
The errors are those of the lock endpoints, and suspension also works with MSC3861 delegated authentication.

## GET `/_synapse/admin/v1/room_reports`

Lists room reports (MSC4151), newest first, as Synapse does.
The optional `from` (offset, default 0), `limit` (default 100), `dir` (`b`, the default, or `f` for oldest first), `user_id` (the reporter) and `room_id` filter the list.
The response holds `room_reports` (each with `id`, `received_ts`, `room_id`, `name`, `canonical_alias`, `user_id` and `reason`), `total`, and `next_token` when more reports follow.

## GET `/_matrix/client/v3/admin/whois/{userId}`

From the [Matrix Spec](https://spec.matrix.org/v1.3/client-server-api/#get_matrixclientv3adminwhoisuserid).
Gets information about a particular user.
`userId` is the full user ID (e.g. `@alice:domain.com`)
