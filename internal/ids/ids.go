// Package ids makes and checks Likho ids: a prefix and a ULID, for example med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G.
package ids

import (
	"crypto/rand"
	"regexp"
	"time"

	"github.com/oklog/ulid/v2"
)

var pattern = regexp.MustCompile(`^[a-z]{3}_[0-9A-HJKMNP-TV-Z]{26}$`)

// New returns a new id with the given prefix ("med", "rec", ...). Ids sort by creation time.
func New(prefix string) string {
	return prefix + "_" + ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}

// Valid reports whether id is a Likho id with the given prefix.
func Valid(id, prefix string) bool {
	return len(id) == len(prefix)+27 && id[:len(prefix)+1] == prefix+"_" && pattern.MatchString(id)
}
