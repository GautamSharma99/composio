---
'@composio/core': patch
---

Bound realtime trigger chunk reassembly so incomplete or malformed messages cannot accumulate for the lifetime of a subscription. Chunk buffers now expire after 60 seconds, reject out-of-range indices, cap concurrent pending events, require every index through the final chunk before dispatch, and are cleared on unsubscribe.
