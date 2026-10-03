// Package links makes and checks the signed links this service hands out.
//
// The object store is never reachable from outside. A browser or a worker gets a link to
// this service instead, valid for a short time and for one thing only: uploading one file,
// or reading one kind of object of one media id. The signature is an HMAC-SHA256 over the
// link's claims, so the service needs no table of issued links.
package links

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Kinds of object a download link can point at; they are the last part of the path.
const (
	KindOriginal = "original"
	KindAudio    = "audio" // what a browser plays
	KindPeaks    = "peaks"
)

var (
	// ErrExpired means the link was valid once.
	ErrExpired = errors.New("the link has expired")
	// ErrInvalid means the link was not made by this service, or was changed.
	ErrInvalid = errors.New("the link is not valid")
)

// Upload is what an upload link stands for. It travels inside the link.
type Upload struct {
	MediaID      string `json:"m"`
	WorkspaceID  string `json:"w"`
	RecordingID  string `json:"r"`
	OriginalName string `json:"n"`
	ContentType  string `json:"c,omitempty"`
	Expires      int64  `json:"e"`
}

// Signer makes and checks links.
type Signer struct {
	secret      []byte
	publicURL   string
	internalURL string
	now         func() time.Time
}

// NewSigner returns a signer. publicURL is the address the links start with, without a trailing slash.
func NewSigner(secret, publicURL string) *Signer {
	return &Signer{secret: []byte(secret), publicURL: publicURL, internalURL: publicURL, now: time.Now}
}

// SetInternalURL gives links to the original a different base: the address other services reach
// this service at inside the network (in a cluster, the service's own name). Only services read
// originals; browsers get the audio and the peaks, which stay on the public address.
func (s *Signer) SetInternalURL(url string) { s.internalURL = url }

// SetClock replaces the clock, for tests.
func (s *Signer) SetClock(now func() time.Time) { s.now = now }

func (s *Signer) sign(message string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(message))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// UploadURL returns the link a file for this upload is sent to with PUT, and when it stops working.
func (s *Signer) UploadURL(upload Upload, ttl time.Duration) (string, time.Time) {
	expires := s.now().Add(ttl)
	upload.Expires = expires.Unix()
	claims, _ := json.Marshal(upload) // a struct of strings and a number always marshals
	payload := base64.RawURLEncoding.EncodeToString(claims)
	token := payload + "." + s.sign("upload\n"+payload)
	return fmt.Sprintf("%s/media/uploads/%s?token=%s", s.publicURL, upload.MediaID, token), expires
}

// VerifyUpload checks the token of an upload link for mediaID and returns what it stands for.
func (s *Signer) VerifyUpload(mediaID, token string) (Upload, error) {
	payload, signature, found := strings.Cut(token, ".")
	if !found || !hmac.Equal([]byte(signature), []byte(s.sign("upload\n"+payload))) {
		return Upload{}, ErrInvalid
	}
	claims, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return Upload{}, ErrInvalid
	}
	var upload Upload
	if err := json.Unmarshal(claims, &upload); err != nil || upload.MediaID != mediaID {
		return Upload{}, ErrInvalid
	}
	if s.now().Unix() > upload.Expires {
		return Upload{}, ErrExpired
	}
	return upload, nil
}

func downloadMessage(mediaID, kind string, expires int64) string {
	return "download\n" + mediaID + "\n" + kind + "\n" + strconv.FormatInt(expires, 10)
}

// DownloadURL returns a link to one kind of object of a media id, and when it stops working.
func (s *Signer) DownloadURL(mediaID, kind string, ttl time.Duration) (string, time.Time) {
	expires := s.now().Add(ttl)
	query := url.Values{}
	query.Set("expires", strconv.FormatInt(expires.Unix(), 10))
	query.Set("signature", s.sign(downloadMessage(mediaID, kind, expires.Unix())))
	base := s.publicURL
	if kind == KindOriginal {
		base = s.internalURL
	}
	return fmt.Sprintf("%s/media/%s/%s?%s", base, mediaID, kind, query.Encode()), expires
}

// VerifyDownload checks the query of a download link for this media id and kind.
func (s *Signer) VerifyDownload(mediaID, kind string, query url.Values) error {
	expires, err := strconv.ParseInt(query.Get("expires"), 10, 64)
	if err != nil {
		return ErrInvalid
	}
	want := s.sign(downloadMessage(mediaID, kind, expires))
	if !hmac.Equal([]byte(query.Get("signature")), []byte(want)) {
		return ErrInvalid
	}
	if s.now().Unix() > expires {
		return ErrExpired
	}
	return nil
}
