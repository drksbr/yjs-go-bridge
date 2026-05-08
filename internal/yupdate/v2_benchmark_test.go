package yupdate

import (
	"encoding/hex"
	"testing"
)

func BenchmarkV2Conversion(b *testing.B) {
	v1 := mustDecodeBenchmarkHex(b, "010165000401017402686900")
	firstV2 := mustDecodeBenchmarkHex(b, "000002a50100000104060374686901020101000001010000")
	secondV2 := mustDecodeBenchmarkHex(b, "0000048a03a50101020001840301210100000001010000")
	mergedV2 := mustDecodeBenchmarkHex(b, "0000058a03e501000102000384000408042174686941000201010000020100010000")

	for _, bb := range []struct {
		name string
		run  func() ([]byte, error)
	}{
		{
			name: "ConvertUpdateToV1/v2",
			run: func() ([]byte, error) {
				return ConvertUpdateToV1(mergedV2)
			},
		},
		{
			name: "ConvertUpdateToV2/v1",
			run: func() ([]byte, error) {
				return ConvertUpdateToV2(v1)
			},
		},
		{
			name: "ConvertUpdatesToV2/v2_v2",
			run: func() ([]byte, error) {
				return ConvertUpdatesToV2(firstV2, secondV2)
			},
		},
		{
			name: "MergeUpdates/v2_v2",
			run: func() ([]byte, error) {
				return MergeUpdates(firstV2, secondV2)
			},
		},
	} {
		b.Run(bb.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := bb.run(); err != nil {
					b.Fatalf("%s unexpected error: %v", bb.name, err)
				}
			}
		})
	}
}

func BenchmarkV2ContentIDs(b *testing.B) {
	firstV2 := mustDecodeBenchmarkHex(b, "000002a50100000104060374686901020101000001010000")
	secondV2 := mustDecodeBenchmarkHex(b, "0000048a03a50101020001840301210100000001010000")
	mergedV2 := mustDecodeBenchmarkHex(b, "0000058a03e501000102000384000408042174686941000201010000020100010000")

	for _, bb := range []struct {
		name string
		run  func() (*ContentIDs, error)
	}{
		{
			name: "CreateContentIDsFromUpdate/v2",
			run: func() (*ContentIDs, error) {
				return CreateContentIDsFromUpdate(mergedV2)
			},
		},
		{
			name: "ContentIDsFromUpdates/v2_v2",
			run: func() (*ContentIDs, error) {
				return ContentIDsFromUpdates(firstV2, secondV2)
			},
		},
	} {
		b.Run(bb.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := bb.run(); err != nil {
					b.Fatalf("%s unexpected error: %v", bb.name, err)
				}
			}
		})
	}
}

func mustDecodeBenchmarkHex(tb testing.TB, value string) []byte {
	tb.Helper()

	decoded, err := hex.DecodeString(value)
	if err != nil {
		tb.Fatalf("hex.DecodeString(%q) unexpected error: %v", value, err)
	}
	return decoded
}
