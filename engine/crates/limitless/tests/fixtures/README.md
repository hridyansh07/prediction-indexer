# Limitless fixture provenance

- `orderbook_update_live_2026_09_12.json` and
  `system_live_2026_09_12.json` are retained public Socket.IO deliveries from
  `wss://ws.limitless.exchange/markets`, captured on 2026-09-12 after a
  `subscribe_market_prices` request for the named public market. They retain the
  venue payload inside the splice's lossless `{event,data}` wrapper. The update
  demonstrates the production-only sparse `version`, rich initial-book fields,
  JSON-number prices, and raw six-decimal contract quantities.
- `orderbook_update_documented.json` and `new_price_data_documented.json` are
  adapted only by scaling example level sizes to the documented raw six-decimal
  share unit from the authoritative [market-data examples](https://docs.limitless.exchange/developers/websocket/market-data).
  The intentionally large second size reaches the Replay quantity logical limit.
- `market_created_documented.json` and `market_resolved_documented.json` copy the
  authoritative [market-lifecycle examples](https://docs.limitless.exchange/developers/websocket/market-lifecycle)
  inside the same capture wrapper.

The immutable released SDK references used to cross-check these shapes are
TypeScript SDK commit
[`1d5b3a7c44af1bfba97ce7b5bd8c2f859cdfeeba`](https://github.com/limitless-labs-group/limitless-exchange-ts-sdk/tree/1d5b3a7c44af1bfba97ce7b5bd8c2f859cdfeeba)
and Go SDK commit
[`dc96e448ce2055ad9cf7c171be04087c7842cbaa`](https://github.com/limitless-labs-group/limitless-exchange-go-sdk/tree/dc96e448ce2055ad9cf7c171be04087c7842cbaa).
Those SDK releases lag the live wire's `version` and `midpoint`; the retained
delivery and current docs are authority for those fields. The official Python
SDK and CLI identify orderbook `size` as raw shares scaled by 1e6.
