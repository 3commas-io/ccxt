package ccxt

import (
	"encoding/json"
	"testing"
)

// Hyperliquid's `spotMeta.tokens` array is compacted: a token that leaves the
// venue takes its slot with it, so from the first gap onward a token's `index`
// no longer equals its position in the array. `universe[i].tokens` carries
// index values, so resolving them positionally either lands on a different
// token (publishing the market under the wrong ticker) or lands nowhere and
// the market is dropped without a trace.
//
// The fixture below is the smallest shape that reproduces both outcomes:
// tokens hold indices 0, 1 and 5 in a three-element array, so index 5 is past
// the end and index 1 is the last one where position still matches.
//
// Against production data on 2026-09-04 the positional read returned 305 of
// the venue's 326 spot pairs, dropped 21, and mislabeled 3 (WBRL/USDC was
// served as TSLAX/USDC, SPCX/USDC as AIW3/USDC, WARS/USDC as SPCXX/USDC).
const sparseSpotMetaFixture = `[
  {
    "tokens": [
      {"name": "USDC",  "szDecimals": 8, "weiDecimals": 8, "index": 0, "isCanonical": true},
      {"name": "PURR",  "szDecimals": 0, "weiDecimals": 5, "index": 1, "isCanonical": true},
      {"name": "NVDAX", "szDecimals": 2, "weiDecimals": 8, "index": 5, "isCanonical": true}
    ],
    "universe": [
      {"name": "PURR/USDC",  "tokens": [1, 0], "index": 0, "isCanonical": true},
      {"name": "NVDAX/USDC", "tokens": [5, 0], "index": 1, "isCanonical": true}
    ]
  },
  [
    {"dayNtlVlm": "8906.0", "markPx": "0.14",   "midPx": "0.209265", "prevDayPx": "0.20432"},
    {"dayNtlVlm": "89014.0", "markPx": "231.1", "midPx": "231.115",  "prevDayPx": "228.4"}
  ]
]`

func hyperliquidWithFixture(t *testing.T, fixture string) *Hyperliquid {
	t.Helper()
	var decoded any
	if err := json.Unmarshal([]byte(fixture), &decoded); err != nil {
		t.Fatalf("fixture is not valid JSON: %v", err)
	}
	ex := NewHyperliquid(map[string]any{"enableRateLimit": false})
	ex.FetchResponse = decoded
	return ex
}

// A pair whose base token sits past the end of the compacted array must still
// resolve. Before the index-keyed lookup this dropped NVDAX/USDC entirely and
// returned one market instead of two.
func TestFetchSpotMarketsResolvesTokensBeyondArrayLength(t *testing.T) {
	ex := hyperliquidWithFixture(t, sparseSpotMetaFixture)

	markets, err := ex.FetchSpotMarkets()
	if err != nil {
		t.Fatalf("FetchSpotMarkets: %v", err)
	}
	if len(markets) != 2 {
		got := make([]string, 0, len(markets))
		for _, m := range markets {
			got = append(got, *m.Symbol)
		}
		t.Fatalf("expected every universe pair to resolve, got %d of 2: %v", len(markets), got)
	}

	bySymbol := map[string]MarketInterface{}
	for _, m := range markets {
		bySymbol[*m.Symbol] = m
	}
	for _, want := range []string{"PURR/USDC", "NVDAX/USDC"} {
		if _, ok := bySymbol[want]; !ok {
			t.Errorf("market %s missing", want)
		}
	}

	// The venue's own pair name must stay attached to the symbol it was built
	// from — a wrong-token resolution shows up here as a mismatched id.
	if m, ok := bySymbol["NVDAX/USDC"]; ok {
		if m.Id == nil || *m.Id != "NVDAX/USDC" {
			t.Errorf("NVDAX/USDC resolved to venue pair %v, want NVDAX/USDC", m.Id)
		}
		if m.BaseCurrency == nil || *m.BaseCurrency != "NVDAX" {
			t.Errorf("NVDAX/USDC has base %v, want NVDAX", m.BaseCurrency)
		}
	}
}

// The token whose index still matches its position must not regress.
func TestFetchSpotMarketsKeepsAlignedTokens(t *testing.T) {
	ex := hyperliquidWithFixture(t, sparseSpotMetaFixture)

	markets, err := ex.FetchSpotMarkets()
	if err != nil {
		t.Fatalf("FetchSpotMarkets: %v", err)
	}
	for _, m := range markets {
		if *m.Symbol != "PURR/USDC" {
			continue
		}
		if m.Id == nil || *m.Id != "PURR/USDC" {
			t.Fatalf("PURR/USDC resolved to venue pair %v, want PURR/USDC", m.Id)
		}
		return
	}
	t.Fatal("PURR/USDC missing")
}
