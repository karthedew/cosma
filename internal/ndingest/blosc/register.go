package blosc

// DecodeFunc adapts Decode to the internal/ndingest DecodeFunc shape so the
// parent package can register it without this package importing its parent
// (which would create an import cycle). blosc carries all the geometry it needs
// in the frame header, so the codec config is ignored.
//
// internal/ndingest registers this under the codec ID "blosc" in its init.
func DecodeFunc(src []byte, _ map[string]any) ([]byte, error) {
	return Decode(src)
}
