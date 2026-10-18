# Mention compatibility

Explicit user and room mentions use `content.m.mentions` and the `.m.rule.is_user_mention` and `.m.rule.is_room_mention` defaults.
Property conditions compare scalar JSON values exactly and support escaped dots and backslashes in property paths.
Room mentions require the sender's notification power level, including for legacy `@room` messages.

For compatibility with older clients, Zendrite retains the display-name, localpart and `@room` defaults for events without `m.mentions`.
The presence of `m.mentions`, including an empty object or null, suppresses those three legacy rules.
Custom keyword rules continue to work independently of mention metadata.

Stored account rules receive missing explicit mention defaults when read through the push-rule API, account-data API or notification evaluator.
The new user and room defaults inherit the enabled state and actions of the legacy display-name and room rules respectively.
Existing explicit rules and custom rules retain their settings and relative order.
This read-time upgrade avoids overwriting concurrent account-data updates; subsequent client edits persist the upgraded rules normally.
