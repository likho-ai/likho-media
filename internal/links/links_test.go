package links

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

func upload() Upload {
	return Upload{
		MediaID:      "med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G",
		WorkspaceID:  "wsp_01JB7Z5K3M9Q2W4X6Y8A0C1E3G",
		RecordingID:  "rec_01JB7Z5K3M9Q2W4X6Y8A0C1E3G",
		OriginalName: "call 1.mp3",
		ContentType:  "audio/mpeg",
	}
}

func tokenOf(t *testing.T, link string) string {
	t.Helper()
	parsed, err := url.Parse(link)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Query().Get("token")
}

func TestUploadLinkCarriesWhatItStandsFor(t *testing.T) {
	signer := NewSigner("secret", "http://localhost:8080")
	link, expires := signer.UploadURL(upload(), time.Hour)

	if !strings.HasPrefix(link, "http://localhost:8080/media/uploads/med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G?token=") {
		t.Fatalf("unexpected link %s", link)
	}
	if until := time.Until(expires); until < 59*time.Minute || until > time.Hour {
		t.Fatalf("expires in %s, want about an hour", until)
	}
	got, err := signer.VerifyUpload("med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G", tokenOf(t, link))
	if err != nil {
		t.Fatal(err)
	}
	want := upload()
	want.Expires = expires.Unix()
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestUploadLinkIsRefused(t *testing.T) {
	signer := NewSigner("secret", "http://localhost:8080")
	link, _ := signer.UploadURL(upload(), time.Hour)
	token := tokenOf(t, link)
	payload, signature, _ := strings.Cut(token, ".")

	other := upload()
	other.WorkspaceID = "wsp_01JB7Z5K3M9Q2W4X6Y8A0C1E99"
	otherLink, _ := signer.UploadURL(other, time.Hour)
	otherPayload, _, _ := strings.Cut(tokenOf(t, otherLink), ".")

	cases := map[string]struct {
		mediaID, token string
		signer         *Signer
	}{
		"for another media id":        {"med_01JB7Z5K3M9Q2W4X6Y8A0C1E99", token, signer},
		"claims changed":              {upload().MediaID, otherPayload + "." + signature, signer},
		"signature changed":           {upload().MediaID, payload + "." + signature[:len(signature)-2] + "AA", signer},
		"no signature":                {upload().MediaID, payload, signer},
		"empty":                       {upload().MediaID, "", signer},
		"signed with another secret":  {upload().MediaID, token, NewSigner("other", "http://localhost:8080")},
		"not base64":                  {upload().MediaID, "%%%.sig", signer},
		"a download signature reused": {upload().MediaID, payload + "." + signer.sign("download\n"+payload), signer},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := c.signer.VerifyUpload(c.mediaID, c.token); !errors.Is(err, ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
}

func TestLinksExpire(t *testing.T) {
	signer := NewSigner("secret", "http://localhost:8080")
	now := time.Now()
	signer.SetClock(func() time.Time { return now })
	uploadLink, _ := signer.UploadURL(upload(), time.Minute)
	downloadLink, _ := signer.DownloadURL(upload().MediaID, KindAudio, time.Minute)
	query := queryOf(t, downloadLink)

	signer.SetClock(func() time.Time { return now.Add(59 * time.Second) })
	if _, err := signer.VerifyUpload(upload().MediaID, tokenOf(t, uploadLink)); err != nil {
		t.Fatalf("upload link refused before it expired: %v", err)
	}
	if err := signer.VerifyDownload(upload().MediaID, KindAudio, query); err != nil {
		t.Fatalf("download link refused before it expired: %v", err)
	}

	signer.SetClock(func() time.Time { return now.Add(2 * time.Minute) })
	if _, err := signer.VerifyUpload(upload().MediaID, tokenOf(t, uploadLink)); !errors.Is(err, ErrExpired) {
		t.Fatalf("upload: got %v, want ErrExpired", err)
	}
	if err := signer.VerifyDownload(upload().MediaID, KindAudio, query); !errors.Is(err, ErrExpired) {
		t.Fatalf("download: got %v, want ErrExpired", err)
	}
}

func queryOf(t *testing.T, link string) url.Values {
	t.Helper()
	parsed, err := url.Parse(link)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Query()
}

func TestDownloadLinkIsForOneKindOfOneFile(t *testing.T) {
	signer := NewSigner("secret", "http://localhost:8080")
	link, _ := signer.DownloadURL("med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G", KindAudio, time.Minute)
	if !strings.HasPrefix(link, "http://localhost:8080/media/med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G/audio?") {
		t.Fatalf("unexpected link %s", link)
	}
	query := queryOf(t, link)

	if err := signer.VerifyDownload("med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G", KindAudio, query); err != nil {
		t.Fatal(err)
	}
	if err := signer.VerifyDownload("med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G", KindOriginal, query); !errors.Is(err, ErrInvalid) {
		t.Fatalf("another kind: got %v, want ErrInvalid", err)
	}
	if err := signer.VerifyDownload("med_01JB7Z5K3M9Q2W4X6Y8A0C1E99", KindAudio, query); !errors.Is(err, ErrInvalid) {
		t.Fatalf("another file: got %v, want ErrInvalid", err)
	}

	later := url.Values{"expires": {"99999999999"}, "signature": query["signature"]}
	if err := signer.VerifyDownload("med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G", KindAudio, later); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expiry changed: got %v, want ErrInvalid", err)
	}
	if err := signer.VerifyDownload("med_01JB7Z5K3M9Q2W4X6Y8A0C1E3G", KindAudio, url.Values{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("no query: got %v, want ErrInvalid", err)
	}
}
