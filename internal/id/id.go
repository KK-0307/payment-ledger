// Package id generates prefixed, time-sortable identifiers (ULID layout:
// 48-bit millisecond timestamp + 80 random bits, Crockford base32), e.g.
// "pay_01J9Z3V4K8Q2M6T0X7R5B1C9DE".
package id

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// New returns prefix + "_" + a 26-character ULID-style string.
func New(prefix string) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(time.Now().UnixMilli())<<16)
	if _, err := rand.Read(b[6:]); err != nil {
		panic(err)
	}
	return prefix + "_" + encode(b)
}

func encode(b [16]byte) string {
	// 128 bits -> 26 base32 characters (first char carries 3 bits).
	hi := binary.BigEndian.Uint64(b[0:8])
	lo := binary.BigEndian.Uint64(b[8:16])
	var out [26]byte
	for i := 25; i >= 0; i-- {
		out[i] = alphabet[lo&31]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return string(out[:])
}
